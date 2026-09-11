# How to run the tests

[<- back to the main README](../README.md)

From the repository root:

```sh
pip install -ve .[test,dev]
pytest
```

The suite needs no Slurm cluster.
The suite takes about 55s end to end.
Everything but the botorch tests takes about 26s,
and GP fits take the rest.
Most of that first 26s goes to one ds-service process per test.
A test against the real queue costs that time.

The `[test]` extra installs botorch, and so torch.
That download is large.
Without the extra, `test_optimize_space_botorch.py` skips,
and the rest of the suite still runs.
`[dev]` adds `black` and `pyright`.
The repository conventions require a clean run of both,
alongside a passing suite.

## What is real and what is mocked

**Slurm is mocked.**
`FakeSlurm` (in `tests/conftest.py`) replaces the `subprocess` module
inside `slurm_utils`, and intercepts `sbatch`, `squeue` and `scancel`.
Everything above that boundary is the real code path:
script rendering, job-id parsing and environment scrubbing.
A test can inspect the scripts the executor sent to `sbatch`
(`fake_slurm.submissions`).
A test can also inject a command failure
(`fake_slurm.fail_command("sbatch")`).

**ds-service is real.**
Each test gets its own server process on a random port.
So the tests exercise the queue against the real implementation,
not against a stand-in that can drift from it.
The server is in-memory,
so a fresh process per test also means no state leaks between tests.

`DsServiceServer` from `ds-service-client` starts the server.
`DsServiceServer` also decides where the binary comes from:
`$DS_SERVICE_BIN` if you set it, otherwise `ds-service` on `$PATH`.
`$DS_SERVICE_BIN` can be a whole command line rather than a path.

If neither finds it, the tests that need a queue skip.
The template and `slurm_utils` tests still run.

## Layout

Paths are relative to [`tests/`](../tests).

| File | Covers |
| --- | --- |
| `test_templates.py` | The multi-template-per-file loader and every template |
| `test_slurm_utils.py` | `sbatch`/`squeue`/`scancel` wrappers, `get_clean_environ` |
| `test_executor.py` | `SlurmPilotExecutor`: worker groups, scaling, submit/poll, lifecycle |
| `test_worker.py` | `PilotWorkerProcess` and the `slurm-pilot-worker` CLI |
| `test_monitors.py` | The host and cgroup samplers and the monitor threads |
| `test_swtop.py` | The `swtop` monitor: what it collects, how it renders as text, and the CLI |
| `test_swtop_tui.py` | The Textual app: table updates, what each block shows, and polling |
| `test_search_space.py` | The range types and the unit cube mapping (no botorch needed) |
| `test_explore_space.py` | `ExploreSpaceSobolQMC`: the design it draws and what it records (no botorch needed) |
| `test_utils.py` | The shared helpers |
| `test_optimize_space_botorch.py` | `OptimizeSpaceBotorch`: the observations it starts from, rounds, acquisition, search behavior, resuming (skips without botorch) |
| `conftest.py` | Fixtures: real ds-service, fake Slurm, executor, hang guards |
| `worker_harness.py` | Runs a real worker's main loop for a bounded number of tasks, or of queue polls |
| `support_actor.py` | Actor classes; must stay importable by name for actor tests |

## Notes for future changes

- **Worker tests run a real worker.**
  `PilotWorkerProcess.main()` loops forever by design,
  and swallows every `Exception`, so a bad task cannot kill a worker.
  `run_worker()` stops it with a `BaseException` from `task_done`,
  after the expected number of tasks.
  That is why `StopWorker` is not an `Exception`.
  `poll_worker()` counts `task_get` calls instead of completions,
  which is the only way to bound a worker with nothing to run.
  An empty queue completes no tasks,
  so `run_worker` never reaches its limit.
- **The tests drive the Textual app headlessly.**
  `test_swtop_tui.py` runs each scenario through `App.run_test()`
  inside `asyncio.run`, so the suite needs no async plugin.
  A Textual worker runs each poll,
  so a test that waits for a poll waits
  on `app.workers.wait_for_complete()`, not on a sleep.
  The collector's client belongs to the loop that made it.
  So a test against the real server builds the client
  inside the scenario (`open_collector`).
  `test_swtop.py` keeps one loop per test,
  so it can poll a collector twice.
- **A wall-clock alarm bounds every test.**
  The executor's polling loop and the worker's main loop
  both run until a condition holds.
  So a regression turns a failing test into a hanging one.
  An autouse 60s alarm limits every test,
  and the polling tests use a tighter explicit `time_limit` fixture.
- **The botorch tests mostly use a stand-in executor.**
  `LocalExecutor` runs the objective inline.
  The optimizer's contract with the executor is two calls wide:
  `submit` returns a `Task`, and `wait` fills in its `output`.
  A GP fit already dominates each test,
  so a queue round trip adds nothing.
  `TestRealExecutor` keeps the stand-in honest,
  and runs a whole optimization
  through the real executor, the real queue and a real worker.
  The test runs in a thread,
  because the optimizer blocks in `wait` the moment it submits.
- **Four botorch tests assert search behavior, not bookkeeping.**
  They catch a flipped sign on the objective:
  botorch maximizes, and the optimizer minimizes.
  These tests are stochastic,
  because torch's global RNG stays unseeded.
  Their margins come from measured spreads:

    - The monotone case has a median search point of 0.00
      against a 0.5 threshold,
      and a flipped sign puts that point at 0.97 or above.
    - `test_search_beats_random_search` won 12 runs out of 12,
      with a 4.6x margin.

  Assert on that median rather than the max,
  because `qLogNoisyExpectedImprovement` probes away from the incumbent
  by design.
  Single points reach 1.0 on a correct run.
  All four use unimodal objectives on purpose:
  an earlier Himmelblau version of the random-search comparison
  lost 1 run in 10.
- **Queue name == worker group name.**
  Only the workers in group `cpu` serve a task on queue `cpu`.
  The tests rely on this rule to keep groups isolated.
- **Do not wait on an RPC to detect server readiness.**
  A failed first RPC puts the gRPC channel into a ~1s reconnect backoff.
  `DsServiceServer.wait_until_ready()` polls the TCP socket instead.
  For this reason, `conftest` calls it rather than a probe of its own.
  An RPC probe makes the suite ~100x slower.
- **Server lifecycle belongs to `ds-service-client`, not to `conftest`.**
  `DsServiceServer` finds the binary, picks a free port,
  waits for the socket and terminates the process.
  `conftest` only chooses the interface to bind,
  and translates a missing binary into a skip.
- **The test server binds `lo`.**
  `DsServiceServer` takes an interface name rather than an address.
  Loopback keeps a test's queue unreachable from outside the machine.
