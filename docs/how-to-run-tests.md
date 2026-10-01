# How to run the tests

[<- back to the main README](../README.md)

From the repository root:

```sh
pip install -ve .[test,dev]
pytest
```

To run one file, one class or one test, give pytest its node id:

```sh
pytest tests/test_templates.py
pytest tests/test_templates.py::TestParseFile
pytest tests/test_templates.py::TestParseFile::test_a_body_is_stripped
```

The suite needs no Slurm cluster.
It runs about 540 tests, and takes about 35 seconds end to end.
Most tests start a `ds-service` server of their own,
and a test against the real server pays for that start.

The checks that format, type-check and test a change
are under [Conventions](developer-notes.md#conventions) in the developer notes.

## What is real and what is mocked

**Slurm is mocked.**
`FakeSlurm` (in `slurm_workflows.testing`) replaces the `subprocess` module
inside `slurm_utils`, and intercepts `sbatch`, `squeue` and `scancel`.
Everything above that boundary is the real code path:
script rendering, job-id parsing and environment scrubbing.
A test can inspect the scripts the executor sent to `sbatch`
(`fake_slurm.submissions`).
A test can also inject a command failure
(`fake_slurm.fail_command("sbatch")`).

**NVML is mocked.**
`FakeNvml` (in `tests/conftest.py`) replaces the `pynvml` functions
the GPU monitor calls, in every test.
It lists no GPU until a test adds a `FakeGpu` to `fake_nvml.gpus`.
So a machine with GPUs runs the suite as one without.
The patch reaches only the test's own process.
A worker that a test starts in another process
reads the real NVML.

**`ds-service` is real.**
Each test gets its own server process on a random port.
The tests run against the real server,
not against a stand-in that can drift from it.
The server keeps its state in memory,
so a fresh process per test also means no state leaks between tests.

`DsServiceServer` from `ds-service-client` starts the server.
`DsServiceServer` also decides where the binary comes from:
`$DS_SERVICE_BIN` if you set it, otherwise `ds-service` on `$PATH`.
`$DS_SERVICE_BIN` can be a whole command line rather than a path.

If neither one finds the binary, the tests that need a server skip.
The tests that need no server still run,
such as the template and `slurm_utils` tests.

## The plugin other packages load

The fixtures and helpers live in the package,
in the pytest plugin `src/slurm_workflows/testing.py`.
This suite loads it from `tests/conftest.py`,
and a package that builds on `slurm-workflows` loads it the same way:

```python
pytest_plugins = ["slurm_workflows.testing"]
```

The names it provides are the ones in `testing.__all__`.
The [developer notes](developer-notes.md#the-test-plugin-testingpy)
say what belongs in the plugin.

## Layout

Paths are relative to [`tests/`](../tests).

| File | Covers |
| --- | --- |
| `test_templates.py` | The multi-template-per-file loader and every template |
| `test_slurm_utils.py` | `sbatch`/`squeue`/`scancel` wrappers, `get_clean_environ` |
| `test_executor.py` | `SlurmPilotExecutor`: job groups, scaling, submit/poll, lifecycle |
| `test_map_reduce.py` | `SlurmPilotExecutor.map_reduce`: item and map tasks, the fold, the reduce task, and the item queue |
| `test_map.py` | `SlurmPilotExecutor.map`: item and map tasks, the order of the values, and the item queue |
| `test_worker.py` | `PilotWorker` and the `slurm-pilot-worker` CLI |
| `test_monitors.py` | The host, cgroup and GPU samplers and the monitor threads |
| `test_swtop.py` | The `swtop` collector: what it collects, how it renders as text, and the CLI |
| `test_swtop_tui.py` | The Textual app and its widgets: table updates, what each block shows, the layout and the keys, embedding in another app, and polling |
| `test_package.py` | What the package root exports, and the `ImportError` for each name that moved to `slurm-workflows-optimize` |
| `conftest.py` | Loads the plugin, and holds the fixtures private to this suite: fake NVML, hang guards, `map_task_env`, and in-process workers in a thread |
| `../src/slurm_workflows/testing.py` | The plugin: real `ds-service`, fake Slurm, executor, and the helpers that run a real worker's main loop for a bounded number of tasks, or of queue polls, or until it returns for a restart |
| `support_actor.py` | Actor classes. They must stay importable by name for the actor tests |
| `support_map.py` | Helpers the `map_reduce` and `map` tests share |

## Notes for future changes

- **Worker tests run a real worker.**
  `PilotWorker.main()` loops until it sees a restart request.
  It swallows every `Exception`,
  so a bad task cannot kill a worker.
  `run_worker()` stops it with a `BaseException` from `task_done`,
  after the expected number of tasks.
  For this reason, `StopWorker` is not an `Exception`.
  `poll_worker()` counts `task_get` calls instead of completions.
  No other count can bound a worker with nothing to run.
  An empty queue completes no tasks,
  so `run_worker()` never reaches its limit.
- **The tests drive the Textual app headlessly.**
  `test_swtop_tui.py` runs each scenario through `App.run_test()`
  inside `asyncio.run`, so the suite needs no async plugin.
  A Textual worker runs each poll,
  so a test that waits for a poll
  waits on `app.workers.wait_for_complete()`, not on a sleep.
  The collector's client belongs to the event loop that made it.
  So a test against the real server builds the client
  inside the scenario (`open_collector`).
  `test_swtop.py` keeps one event loop per test,
  so it can poll a collector twice.
- **A wall-clock alarm bounds every test.**
  The executor's polling loop and the worker's main loop
  both run until a condition holds.
  So a regression turns a failing test into a hanging one.
  An autouse alarm limits every test to 60 seconds.
  The polling tests use a tighter explicit `time_limit` fixture.
- **A test whose driver blocks runs its real worker in a thread.**
  `map_reduce` and `map` block in a wait the moment they submit their tasks.
  So nothing on the test's own thread can run the worker.
- **The tests keep job groups apart by queue name.**
  See "Task flow" in the developer notes.
