# Developer notes

[<- back to the main README](../README.md)

Notes for people who work on `slurm-workflows` itself.
Everything here is about the code.
The guides the README indexes cover how to *use* the library.

`slurm-workflows` is a Python library (>=3.12)
of helpers for running work on Slurm HPC clusters.
It holds the pilot-job executor, and `swtop`, which watches a run.
The Sobol' exploration and the batch Bayesian search
live in a separate package, `slurm-workflows-optimize`,
which builds on this one.
[The public surface other packages build on](#the-public-surface-other-packages-build-on)
says what that package depends on.

## Where documentation goes

User documentation lives under `docs/`,
organized by [Diataxis](https://diataxis.fr/) type.
The README is a landing page:
what the library is, how to install it,
one minimal usage example, and the index of everything else.
The README links every document, so its table is the one index.
This file does not keep a second copy of it.

```
docs/
  tutorials/        lessons: a learner runs a worked example end to end
  how-to-guides/    directions: a competent user solving one stated problem
  reference/        neutral description, one page per piece of the public surface
  explanation/      why the code is the way it is, for users
  developer-notes.md, terminology.md, how-to-run-tests.md    for contributors
```

For new user-facing documentation,
decide which of the four types it is before you decide where it goes.
Then give it a row in the README's table,
among the documents of its own type.
Keep each document inside its type.
A tutorial that stops to explain links to `explanation/` instead.
A reference page describes rather than recommends.

A reference page covers one thing a user reaches for.
The page takes its name from that class, that command or that subject,
rather than from the module that holds it.
`swtop.md` covers `swtop.py` and `swtop_tui.py` together,
and `swtop-widgets.md` covers `swtop_widgets.py`.
The user documentation describes what the worker and the monitors publish
where a user meets it, rather than under its own module.
A new public class or command needs a page under `reference/`
and a row in the README's table.

Nothing user-facing goes in `README.md` beyond that list.

### Docstrings, comments and this file

Prose in the source is not a third documentation set.
Each kind of prose has one job:

| Where | Carries | Never carries |
| --- | --- | --- |
| Docstring | What a caller needs: what it does, its arguments, what comes back, what it raises | Why it was built this way, how it is implemented, anything a caller cannot act on |
| Comment | What is not obvious at that line, in a sentence or two | An argument for the design, or a paragraph the docs already carry |
| `docs/` | How to use the library, and what it does | |
| This file | Why the code is the way it is: the invariants, the trade-offs, the alternatives that were tried | |

A private helper's docstring is one line that says what it does.

Where a design decision sits in both places,
the next editor changes only one of them.

## Commands

There is no CI in this repository, so nothing runs these commands for you.
To install the package with its extras and run the test suite,
see [`how-to-run-tests.md`](how-to-run-tests.md).
The formatter and the type checker run from the repository root:

```sh
black src tests examples    # format
pyright                     # type-check
```

To build the sdist and the wheel into `dist/`,
run `python -m build`.
Then run `python -m twine check dist/*.tar.gz dist/*.whl`.
`scripts/pb-dev.sh build-python-package` runs both.
Neither `build` nor `twine` is in an extra,
so install them first.
`scripts/pb-dev.sh upload-python-package` uploads the sdist and the wheel in `dist/`
with `twine`.

`pyproject.toml` configures `black` and `pyright`.
`[tool.pyright]` sets the include paths and `pythonVersion`.
For this reason, run `pyright` bare.
Do not pass paths to `pyright`, or it ignores that configuration.

`pyproject.toml` defines two console entry points.
`slurm-pilot-worker` is internal:
generated sbatch scripts invoke it on the compute nodes,
and users never call it directly.
`swtop` is for users.
The [`swtop` reference](reference/swtop.md) describes it.

To run the library, start a `ds-service` server.
`DsServiceServer` can launch one.
Then, on a node that can call `sbatch`,
run a driver script, such as one under `examples/`.

Deploy to clusters with `cpush`.
See `.cpush.json5` for the `rivanna` remote.

## Where things live

Paths are relative to `src/slurm_workflows/`.

| Module | Holds |
| --- | --- |
| `__init__.py` | The public API of the library, and the errors that name the new home of each name that moved to `slurm-workflows-optimize`. An app that embeds `swtop` imports from `swtop` and `swtop_widgets` instead. |
| `slurm_pilot_executor.py` | The driver side, entry point `SlurmPilotExecutor`: job groups, `map` and `map_reduce`, submitting tasks and waiting on them. Depends on `slurm_utils`, `templates/` and `utils`, and on `slurm_pilot_worker` for the worker's actor and its shared constants. |
| `slurm_pilot_worker.py` | The worker side, entry point the `slurm-pilot-worker` command that generated scripts run on compute nodes. Starts the monitors. |
| `slurm_utils.py` | Calls to the Slurm commands, and the environment `sbatch` runs in |
| `monitors.py` | Host, cgroup and GPU sampling, and the threads that publish it. The worker starts the monitors, and `swtop` imports the host and job series prefixes. `swtop` reads no GPU series. |
| `swtop.py` | `swtop`, entry point the `swtop` command: reading a run from the server, and the text frames. |
| `swtop_widgets.py` | The Textual widgets and the poller that `swtop` and any embedding app lay out. Depends on `swtop`. |
| `swtop_tui.py` | The Textual app `swtop` runs in: the layout of the widgets, and the keys |
| `templates/` | Jinja templates for the generated scripts, and their loader |
| `testing.py` | The pytest plugin: a real server, a fake Slurm, an executor wired to both, and helpers that run a real worker in-process. Imports `pytest`, so nothing in the package imports it. |
| `utils.py` | Helpers shared across modules: the remote error record, ids, and the log format |

`tests/` holds the suite.
[`how-to-run-tests.md`](how-to-run-tests.md#layout) maps its files.
`examples/` holds the scripts the tutorials walk through.
`extra/` holds the README's banner image
and a FoxyProxy configuration for Rivanna.
`scripts/pb-dev.sh` is the author's own wrapper for building and uploading the package.
Nothing else depends on it.

`MANIFEST.in` decides what the sdist ships:
the `.py` and `.jinja` files under `src/slurm_workflows`,
the README, the license and `pyproject.toml`.
A package data file of any other type needs a line there,
or an installed copy runs without it.

The driver and the workers never talk to each other directly.
They talk only through the `ds-service` server,
via `DsServiceClient` from the external `ds-service-client` package.
`swtop` uses `DsServiceClientAsync` instead,
for the reason given under Monitoring.
`DsServiceClientAsync` is the asyncio client of the same package,
with the same API.
`DsServiceServer` (same package) can launch a local server process.
`SlurmPilotExecutor` always takes the address as its `server_address` argument.

The server and the client carry the same version.
`pyproject.toml` records the client floor, `>=7.0.0`,
and not an exact version.
Install the `ds-service` release
with the same version as the installed client.

## Tools and libraries

`pyproject.toml` lists the dependencies and the extras.
This table says what each one is here for.

| Dependency | Used by | For |
| --- | --- | --- |
| `ds-service-client` | executor, worker, `monitors`, `swtop`, `testing` | The `ds-service` server's client and its `DsServiceServer` launcher. The one channel between the driver and the workers. It also provides `task_search_id`, which is how `swtop` lists the tasks on a server. |
| `cloudpickle` | `slurm_pilot_executor`, `slurm_pilot_worker` | Serializing functions, arguments and return values, so a locally defined function can cross to a compute node. |
| `jinja2` | `templates/` | Rendering the worker shell script and its sbatch wrapper. |
| `json5` | `templates/` | Parsing the `{#- name: ... -#}` headers of the multi-template files. |
| `textual` | `swtop_widgets`, `swtop_tui` | The `swtop` terminal UI, and the widgets other apps embed. |
| `click` | `swtop`, `slurm_pilot_worker` | Both console entry points. |
| `psutil` | `monitors` | Host and process sampling. |
| `nvidia-ml-py` | `monitors` | GPU sampling through NVML, imported as `pynvml`. |
| `platformdirs` | `slurm_pilot_executor` | Locating the per-user cache dir a run's `work_dir` defaults into. |
| `typeguard` | `slurm_pilot_executor` | `@typechecked` on the public surface. |

Development tooling, behind the `dev` and `test` extras:

| Tool | Extra | Role |
| --- | --- | --- |
| `pytest` | `test` | The suite, and the plugin in `testing.py`. See [`how-to-run-tests.md`](how-to-run-tests.md). |
| `black` | `dev` | Formatting. Configured in `pyproject.toml`. |
| `pyright` | `dev` | Type checking. Configured in `pyproject.toml`. Run it bare. |
| `setuptools_scm` | build | Deriving the version from git tags, with a `1.0.0-dev` fallback. |
| `build` | none, installed by hand | Building the sdist and the wheel. See `scripts/pb-dev.sh`. |
| `twine` | none, installed by hand | Checking and uploading the sdist and the wheel. |
| `cpush` | external | Deploying to clusters. See `.cpush.json5`. |

There is no linter beyond `pyright`.

## Invariants

This section lists the things that are easy to break and quiet when broken.

### Task flow

`as_completed` and `wait` poll `task_get_status`
with every still-pending id in one batched call.

**A job group's queue has the group's name.**
Only the workers of job group `cpu` serve a task on queue `cpu`.

**`_as_completed` drops a task that can never run, not the batch.**
`_starved_tasks` and `_stranded_tasks` return the subset they object to,
rather than raise, because the answer is always a subset.
A queue that nobody scaled says nothing about the queues that somebody scaled.
Pilot jobs in one job group that reach their time limit
say nothing about a task on another job group's queue.

A wait that abandons the rest of `pending`
loses task outputs the server already has.
Under `RAISE_NEVER`, it loses them silently.
Such a wait also breaks what `RAISE_AFTER_COMPLETED` promises.
The failure count in the deferred exception counts *tasks* for the same reason:
one message covers every task on a dead queue.

**The executor warns about every failure, whatever `RaiseOnError` says.**
The warning is the part a caller cannot disable,
because `RAISE_NEVER` otherwise loses a failure entirely.
`task.output` is the only other record,
and nothing forces a caller to read it.
The warning goes to stderr, so it does not land in a caller's stdout.

**A wait publishes its progress, but it does not draw it.**
`wait` and `as_completed` write the `progress_display` key
and append to `progress:<uuid4>` as tasks return.
`swtop` is what turns that into a bar.
The driver prints nothing,
so a run under `nohup` leaves no progress bar in its output file.
A run that someone watches from another shell shows one.

While tasks return, a wait appends the count at most once a second,
so the cost does not grow with the batch.
A wait appends the final count even when it raises,
since the exception says nothing about how far it got.

**A worker marks a task that raised as `Failed`.**
It calls `task_done` with `failed=True`,
so the server fails every task that waits on it.
A task that the server failed for this reason never ran.
Its output is the plain text `Dependency failed (task_id=...)`,
not a cloudpickle.
The poll loop checks for that prefix before it unpickles.

**`task_done` is per worker.**
The worker passes its own `worker_id`,
and the server refuses the call from any other worker.
The id given to `task_done`
must be the id of the worker that claimed the task in `task_get`.

**One executor per `ds-service` server.**
A server holds one flat namespace with no executor in it.
The namespace covers these things:

- the queues
- the `pilot_job_info:`, `pilot_job_start:`, `pilot_job_exit:`,
  `worker_info:`, `worker_exit:`, `task_name:`, `actor_class_args:`,
  `actor_class_kwargs:` and `progress_display` keys
- the progress series, the monitor series and the `slurm_job_gpu_info:` keys
- the monitor counters and the `restart_generation:` counters

Because that namespace names no executor,
the design assumes a server belongs to a single executor.
Two executors on one server share queues by group name
and overwrite each other's actor arguments.

Nothing enforces this rule, because the executor cannot see another one.
That blindness is also why `_starved_tasks` and `_stranded_tasks`
refuse a queue served by jobs this executor did not start.
Task ids and worker ids are still executor-prefixed,
because a *cluster* holds many runs even when a server holds one.

**The payload decides how the worker resolves a task's function.**
`main` looks up a `str` on the actor, and calls a callable as it is.
The check is the type of what `task.function` unpickles to,
never whether the job group has an actor.
A job group with an actor therefore still runs a plain callable,
which is what lets `map_reduce` and `map` submit their own task functions there.
A `str` with no actor to find it on raises,
rather than fail later as a call on a string.

**The worker publishes its actor to the process, as `current_actor()`.**
A task the worker runs has no argument that carries the actor,
so `_map_reduce_task` and `_map_task` read it from the module.
One worker is one process and one actor, so a module global holds it.
`close()` clears it, and only when the actor it holds is this worker's own,
because a test builds two workers in one process.

**Actor constructor arguments travel through the map.**
`define_job_group` cloudpickles `actor_class_args` and `actor_class_kwargs`
into `actor_class_args:<group>` and `actor_class_kwargs:<group>`,
and `PilotWorker.__init__` reads them back under the same names.
The two sides agree by convention alone,
so the key format is part of the contract.
If the key format changes in one place only,
workers silently construct actors with default arguments.

`define_job_group` writes a key only when the caller gives a value.
For this reason, the worker treats `KeyError` as "the caller passed none"
rather than an error.
The `JobGroup` deliberately does not keep the values,
so a redefinition check never sees them.
The map is the only copy, and the last `define_job_group` call wins.
The key holds the job group name and not the executor's,
which is safe only because a server belongs to one executor.

**The executor's name is its identity.**
The name prefixes task ids, pilot job names, script file names and the log,
and it keys the executor's logger.
Two live executors that share a name collide on all of those.
Their log lines land in both work dirs,
even when each executor has a server to itself.
`SlurmPilotExecutor` validates the name
(`[A-Za-z][A-Za-z0-9_-]*`, at least 3 characters).

**A run publishes itself in two halves, one key each.**
`SlurmPilotExecutor._add_job` publishes `pilot_job_info:<job-name>`
through `_publish_pilot_job`
as soon as `sbatch` returns, so a queued job is visible before it runs.
`PilotWorker.__init__` writes `worker_info:<worker-id>`,
a JSON object, before it builds the actor.
A worker that dies in its actor's constructor
therefore still records which job and node it died on.

Each is one key and not one per field, because `swtop` caches what it reads.
A reader that lands between two writes otherwise remembers
a worker whose hostname it never learned.
Nothing ever updates either key, which is what makes both cacheable.

`WORKER_INFO_PREFIX` lives in `slurm_pilot_worker.py`
and `PILOT_JOB_INFO_PREFIX` in `slurm_pilot_executor.py`.
`swtop.py` imports both, so the writers and the reader cannot drift apart.
Nothing deletes either key:
the map is in memory and dies with the server,
which is the only cleanup there is.

**Start and exit times are keys of their own, each written once.**
The batch script writes `pilot_job_start:<job-name>`
and `pilot_job_exit:<job-name>`
through `slurm-pilot-worker --pilot-job-event`.
`PilotWorker.close()` writes `worker_exit:<worker-id>`,
and so does `PilotWorker.__init__` when the actor fails to build.
A worker's start time is the `start_time` field of its `worker_info:` key.
An exit written into the `pilot_job_info:` or `worker_info:` key
breaks the cache in `swtop`, so do not merge them.

`swtop` hides a pilot job or a worker once its exit key exists.
It also hides the Slurm job of a pilot job that exited.
`swtop` finds those keys with one `map_search_key` per prefix per poll,
rather than a read per key.
`swtop` shows a pilot job with no `pilot_job_start:` key as not yet started.
The prefixes live in `slurm_pilot_worker.py`, beside `WORKER_INFO_PREFIX`.

**Task names are UTF-8 in the map, not pickles.**
`set_task_name` writes `task_name:<task_id>` as encoded text,
unlike the actor arguments beside it.
The reason is that a name is a string,
and something other than this library has to read it.

### Restarting workers

`SlurmPilotExecutor.restart_jobs` advances the counter `restart_generation:<group>`.
Each worker reads it at startup and checks it between tasks.
A worker that sees it move returns from `main()`.
Then `close()` publishes its exit,
and the entry point exits with `RESTART_EXIT_CODE` (75).
The worker script runs the entry point again on that status,
and the new process imports the code from disk.
The executor and the worker share nothing else,
so the restart travels through the server like everything else.

**The worker reads the counter before it publishes its identity.**
`PilotWorker.__init__` reads it before it imports the actor class
or writes `worker_info:`.
The worker imports task functions later still, as each task unpickles.
The generation a worker records is therefore never newer
than the actor and task code it loaded.
If the worker reads it later, a restart requested between the import and the read
is lost on that worker.
That worker records the new generation on the old code,
so it never restarts, and the wait counts it as new.

The same order makes `restart_generation` in `worker_info:` safe to wait on.
A worker that published an old generation is one that must still exit.
The entry point imports `slurm_workflows` itself before that read.
A restart requested in that short window
reaches the worker's actor and tasks, but not the library.

**The first check comes before the first claim.**
The `wait=True` guarantee rests on this order.
The wait scans `worker_info:` keys, and it returns once no old worker is live.

A worker that read the old generation
but did not publish `worker_info:` yet is invisible to that scan.
The wait can return before that worker publishes.
That worker then checks before it claims anything,
sees the new generation, and exits without running a task.
So every task that a worker claims after the wait returns runs on a new worker.

For this reason, `main()` claims no task until one read of the counter succeeds.
A failed first read leads to a sleep and another read, not a claim.
The rate limit does not apply until a read succeeds.

**A worker checks right after each task, and at most once a second while idle.**
The check right after a task keeps a busy worker
from claiming one more task on the old code.

The rate limit, `RESTART_CHECK_INTERVAL_S`, bounds what idle workers cost the server.
Without `wait`, this interval is the window
in which a worker can still claim a task or two on the old code.

**Only the worker process is inside the shell loop.**
The worker script sources `/etc/profile`, runs the setup script,
and only then loops over `slurm-pilot-worker`.
The setup script runs once per Slurm task.
It also runs once for each pilot job start and exit the batch script publishes.
Each run is in a bash of its own.

Nothing makes a setup script safe to run twice.
For a batch worker, the batch shell sources the worker script,
so a second run stacks its effects in that shell.
A new worker therefore inherits the environment the setup script built.
A change to the setup script or to `sbatch_args` needs new pilot jobs.

**The restart generation is part of the monitor election key.**
Counters never reset while the server runs.
A restart replaces every worker of a job, the elected one included,
and the elected worker's monitors stop in `close()`.
With `host_monitor:<hostname>:<job-id>` alone,
the counter tells every new worker a value past 1,
and the job has no monitor after its first restart.
With the generation in the key, the new workers hold a fresh election.

During a restart, an old and a new worker can both sample one node.
Two workers of one job that read different generations do the same.
Either way, the second worker only adds points to the same time series.

**The wait tracks worker ids, not pilot jobs.**
`_wait_for_restart` sorts each `worker_info:` key once.
The key names a new worker, an old one to wait on,
or one in a pilot job this job group does not track.

An old worker is done when its `worker_exit:` key exists,
or when `squeue` no longer lists its Slurm job.
A worker killed with SIGKILL publishes no exit,
so the `squeue` check is what ends the wait for it.
A failed `squeue` keeps the last answer and does not call the job dead,
as in the liveness checks of a wait.
A timeout raises but leaves the counter where it is,
since the executor cannot lower the counter again.

### `map_reduce` and `map`

`map` hands out items the way `map_reduce` does,
and both calls share the helpers that do it.
Every rule in this section holds for both calls,
unless the rule names one of them.

**A call puts every item task on the item queue
before it submits the first map task.**
This ordering is the whole basis of the call,
and it is what lets a map task read `NoTaskAvailable` as "the work is done".
`task_get` raises it when no queue it polled has a *ready* task.
On its own, `NoTaskAvailable` means that everything is claimed,
not that nothing more arrives.

The ordering supplies the other half.
No map task can run before every item task exists.
Also, the call enqueues nothing on the item queue afterward.
`_enqueue_items` returns only after the last `task_add`,
and `_submit_map_tasks` runs after it.

A change that streams the iterable,
or that enqueues more item tasks later,
breaks that ordering.
A fast map task then drains what is there and sees an empty item queue.
Under `map_reduce`, the call returns a partial result
that covers part of the input, and nothing raises.
Under `map`, the call finds items with no value,
and raises `RuntimeError` for them rather than return a short list.
For this reason, both calls read the iterable out into a list first.
A test of each call asserts on the order of the `task_add` calls.

**Each call gets an item queue of its own,
and no job group serves it.**
`MAP_REDUCE_QUEUE_TEMPLATE` and `MAP_QUEUE_TEMPLATE` name it,
each with a counter of its own.
Workers poll their own job group's queue only,
so the call's own map tasks drain the item queue and nothing else does.
The item queue also stays clear of `_starved_tasks` and `_stranded_tasks`.
Both look at the queues of the tasks handed to the wait,
never at the item queue.

**A map task builds a client of its own.**
A map task has no handle on the worker's client,
and that client belongs to the worker's own loop in any case.
`DsServiceClient()` reads `DS_SERVER_ADDRESS`,
and the task claims its items under `PILOT_WORKER_ID`.
`PilotWorker.__init__` puts both in the environment.
A test that drives that class gets them,
and one that calls the task function directly sets them itself.

A `with` block closes the client.
A worker runs many tasks over its life.
A leaked gRPC channel per task therefore accumulates over the whole life of the worker.

**A map task marks an item task done only once it has the item's value,
and records an empty output.**
Under `map_reduce` that is after the fold,
and under `map` after `map_fn` returns.
`Finished` on the item queue therefore means "counted",
which is what a reader of the item queue expects.
The value travels on inside the output of the map task that computed it.
A second copy on the item task holds every mapped value on the server twice.

**A map task of `map` returns each value paired with the id of its item task.**
The driver built every item task id when it enqueued the items,
so it maps each id back to the index of its item.
The values therefore land in input order
whichever task claimed them and whenever it finished.
The order of the pairs in a task's output carries no meaning.
A test reverses it to check that the driver does not rely on it.
An id carries the index with no change to the item task's input,
so both calls enqueue items through the same helper.

**`map_reduce` folds the partial results in a reduce task,
never on the driver.**
The call submits one more task on `queue`, `_reduce_task`,
with every map task as a parent.
The server holds it until every map task finishes,
so every partial result is on the server when it starts.
It reads them with `task_get_output`, one at a time and in submission order,
and folds them into its own copy of `init`.
Like a map task, it builds a client of its own.
The driver never loads a partial result,
so it holds neither `reduce_fn`'s dependencies
nor every partial result at once.

### The two interfaces

`SlurmPilotExecutor` offers a simple interface, `map` and `map_reduce`,
and an advanced one, `submit` with `wait` or `as_completed`.
The class source groups its methods by interface.
The setup methods come first: `define_job_group`, `scale_jobs`, `restart_jobs`.
Then come `map_reduce` and `map` and the helpers only they use.
Then come `set_task_name`, `submit`, `as_completed` and `wait`.
Keep a new method in the block of the interface it belongs to.

**`map` and `map_reduce` take the queue and the function first,
and the rest by keyword.**
The order matches `submit`.
`desc` and `num_tasks` have defaults,
because the short form `executor.map(queue, fn, items)` is the one a user meets first.
A default `desc` carries the call's own counter, `map-<n>` or `map_reduce-<n>`,
which is the same `<n>` as in the item queue name.

### The public surface other packages build on

`slurm-workflows-optimize` builds on the advanced interface.
A change to any of the following is a breaking change for that package,
and needs a major version:

- `SlurmPilotExecutor.submit`, with `task_priority`.
- `SlurmPilotExecutor.set_task_name`.
- `SlurmPilotExecutor.wait`, with `raise_on_error`,
  and the guarantee of `RAISE_AFTER_COMPLETED`:
  every task that can finish has its `output` filled in before the call raises.
- `Task.output`, and `RemoteExecutionError` as the value of a failed task.
- `RaiseOnError`.
- The plugin in `testing.py`: every name in its `__all__`.

The search uses `submit` and `wait`, not `map`.
It needs a priority per study and a task name per point.
It also needs the outputs that came back before a failure.

**Other packages import every name the package root exports from the root.**
`slurm-workflows-optimize` imports `SlurmPilotExecutor`, `RaiseOnError`, `Task`
and `RemoteExecutionError` from `slurm_workflows`, never from a module.
A module can therefore move without a break,
as long as the package root keeps the name.

### The test plugin (`testing.py`)

**The plugin is opt-in.**
A suite loads it with `pytest_plugins = ["slurm_workflows.testing"]`.
The package does not register it through the `pytest11` entry point.
An entry point loads the plugin, and its fixture names,
into the suite of every project that installs `slurm-workflows`.

**The plugin holds only what another package needs.**
The fixtures that are private to this suite,
such as the hang guard, the fake NVML and `worker_thread`,
stay in `tests/conftest.py`.
That conftest imports `make_worker` and `run_worker` from the plugin,
so it calls `pytest.register_assert_rewrite` on the plugin first.
Otherwise pytest imports the plugin too early to rewrite its asserts,
and warns.

### Logging

**The executor's logger carries the executor's name**
(`slurm_workflows.executor.<name>`), and does not propagate.
A name shared between executors collects one `FileHandler` per executor.
Every line then lands in every work dir opened in this process,
so the first executor's log fills with the second's records.
`close()` removes and closes the handler.
The logger itself stays in the logging registry, inert.
For this reason, a test that reuses an executor name
inherits whatever handlers the previous one left on it.

### Templates (`templates/`)

`render_template` renders the sbatch and worker shell scripts
from Jinja2 templates in a custom file format.
Each `.jinja` file holds one or more named templates,
each one under a `{#- name: "..." -#}` JSON5 header.
`templates/__init__.py` parses those headers.
A template address is `"<file_prefix>:<name>"`,
for example `"slurm_pilot:worker_script"`.

`render_template` carries `@overload` signatures
that document each template's required keyword arguments.
**When you change a template variable, keep those overloads in sync.**
`restart_exit_code` is one of those template variables.
The executor passes `RESTART_EXIT_CODE` from `slurm_pilot_worker`,
so the worker and the shell loop agree on one constant.
The environment uses `StrictUndefined`, so a missing variable is a hard error.

`{#-` is also how a template body ends,
so a body cannot contain a whitespace-trimming Jinja comment.
The parser reads such a comment as the header of the next template.
Use `{#` without the dash inside a body.

### Monitoring (`monitors.py`, `swtop.py`)

**One worker per job per node per restart generation samples, and a counter decides which.**
`counter_get_next_value` hands out distinct, gap-free values.
The worker told 1 for `host_monitor:<hostname>:<job-id>:<generation>` takes the node,
the part of its job on that node, and the GPUs that part can see.
`<generation>` is the restart generation the worker read at startup.

A job's cgroup is local to each node,
so the job series carry the hostname as well as the job id.
The key carries the job id because counters never reset while the server runs.
Without it, a node that a later pilot job lands on gets no monitor.
Two live jobs on one node both sample it,
and only add points to the same time series.
The section Restarting workers says why the key carries the generation.

The election needs no lock and no designated rank,
and the workers do not need to know each other.
Nothing hands a subject back when that worker dies:
the time series stops, and `swtop` marks it stale.
A re-election needs a heartbeat and a lease, and this design has neither.

**GPUs come from NVML, through `nvidia-ml-py`, in a thread of their own.**
`nvidia-ml-py` is NVIDIA's own binding, and it is pure Python.
It loads the driver's library only when `nvmlInit` runs,
so it installs on a CPU node and costs nothing there.
It reads the GPUs in-process,
where `nvidia-smi` starts a process every interval
and needs its text parsed.

`start_gpu_monitor` counts the GPUs in a short NVML session of its own,
and starts nothing where that fails or finds no GPU.
So a CPU node runs no GPU thread and logs no GPU errors.
The thread then holds one session for as long as it runs.
`GpuMonitor` runs apart from the job monitor,
so a GPU read that stalls does not delay the job's readings.
The GPU's type is text, which a time series cannot hold,
so it goes in the map, under `GPU_INFO_PREFIX`.

**GPU readings do not respect `CUDA_VISIBLE_DEVICES`.**
That variable belongs to the CUDA runtime, and NVML ignores it.
The monitor needs NVML's view anyway.
Slurm sets the variable per task,
so through it the elected worker sees its own GPU and not the job's others.
Where Slurm constrains devices, NVML lists the GPUs of the job step on the node.
Where it does not, NVML lists every GPU on the node.

**Sampling threads are daemons that swallow their errors.**
A monitor must not hold open a worker that Slurm kills at its time limit,
and a failed sample must not end the time series.
A node briefly unreachable is the common case, and a gap beats a stop.
`close()` stops them before it closes the client whose channel they use.

**`swtop` can only show what an RPC can answer.**
`task_get_count_by_state` covers every task,
and `task_search_id` enumerates them.
Nothing enumerates workers, hosts or jobs.
So `swtop` builds those tables from the keys that the workers and the monitors publish.
It finds those keys with a search of the map and the time series.
`swtop` cannot list a worker that did not publish its identity.
That limit belongs to the server, and this library does not work around it.

**`Collector` reads an identity once.**
`Collector` caches every worker's fields,
every pilot job's fields and start time,
and every task's name.
Nothing ever changes any of them after the first write.
It does not cache which pilot jobs and workers exited.
That set only grows,
and one key search per exit prefix, each poll, reads the whole of it.
Without the cache, a 400-worker pool costs 400 reads every 2 seconds,
plus one for every named task.

`Collector` does not cache a name that is not there yet.
`set_task_name` runs just after `submit`,
so a task polled between the two
otherwise stays `-` for the rest of the run.
Each read goes into the cache as soon as it returns,
rather than once the whole poll is done.
A poll cut short, by an unmount or by a caller's timeout,
then still leaves the next one less to read.

**`swtop` draws a failed poll, and does not raise it.**
A display that exits when the server blinks
takes the screen down with it.
In the UI the tables stay as they were,
because the last good reading beats a blank screen
while a server restarts.

**`swtop` reads with the asyncio client.**
A poll is a handful of key searches.
It adds a read per worker, per pilot job, per named task,
per running task and per monitored time series.
One after another, those reads cost a round trip apiece,
and a few hundred workers do not fit in a two-second interval.

`Collector` issues each set of reads with `asyncio.gather`,
so a poll costs about one round trip however wide the pool is.
The cache means `Collector` reads only what is new.
The one round trip per poll holds on a fast network.
Through an ssh tunnel, the first poll of a pool of 2500 workers
and 2500 named tasks took 4.4 s.
Every later poll took about 0.8 s.

**`swtop.py` imports no Textual.**
So the text frames and their tests run without it.
`swtop.py` imports nothing from `swtop_widgets` at the top,
and reaches it only through the lazy import of `swtop_tui` in `watch`.

**Both displays read the same row builders.**
`BLOCKS` in `swtop.py` is the one definition of the blocks.
It gives each block its title, columns, empty message and row builder,
and it sets their order.
The text frames and the UI differ only in how they draw them.

**The tasks tab filters in the widget, not in the collector.**
`TaskTable` selects the tasks in its checked states
from a snapshot that holds every task.
A change of states then redraws the last snapshot at once,
with no poll to wait for.
The filter in the widget also leaves the snapshot the same for every view.
As a result, the text frames, the summary line and an embedding app
still see every task.
The cost is that each poll still reads the state of every task.

**The widgets bind no keys, and set no ids.**
An app that embeds the widgets sets up its own keys,
and a key that a widget binds takes one from that app.
An id that a widget sets can collide with one of the app's.
So only `SwtopApp` binds keys, including the tab keys in `TAB_KEYS`,
and only `SwtopApp` gives ids to its areas.
The panes from `block_pane` are the exception:
a `TabbedContent` needs pane ids,
so their default ids carry an `swtop-` prefix.

Each widget carries its styles in `DEFAULT_CSS`,
since an app that embeds it loads no stylesheet of `swtop`'s.
Those styles select on a widget type or on an `swtop-` class, never on an id.

### Slurm interaction (`slurm_utils.py`)

`is_batch_worker=False` wraps the worker script in `srun`
and passes `--output <work_dir>/<name>-%j-%t.out`.
For this reason, the `worker_sbatch_script` template takes `name` and `work_dir`.
For users, [`reference/what-a-run-publishes.md`](reference/what-a-run-publishes.md#logs)
documents which file holds a worker's log.
This section says why the shell decides it rather than Python.

The generated script drops `--output` for a job of exactly one Slurm task.
The shell decides that at run time,
because Python cannot know the count when it renders the script.
See the comment above `num_slurm_tasks=` in `templates/slurm_pilot.jinja`.

**The batch script traps SIGTERM, and the worker turns it into `SystemExit`.**
Slurm sends SIGTERM to every process of the job
when it cancels the job or the job reaches its time limit.
It sends SIGKILL `KillWait` seconds later.
The TERM trap only calls `exit 143`,
so the job ends with that status rather than dying of the signal.
Then the EXIT trap publishes the pilot job's exit.
Bash 5.2 runs the EXIT trap on an untrapped SIGTERM as well.

Bash runs a trap only after the foreground `srun` returns.
`srun` gets the same SIGTERM, so it returns once its workers exit,
and the pilot job's exit comes after theirs.
Python's default for SIGTERM ends the process without running `finally`.
`slurm_pilot_worker` installs a handler that raises `SystemExit`,
which `PilotWorker.main` does not catch,
so `worker.close()` runs and publishes the worker's exit.
The worker's constructor catches `BaseException` around the actor
for the same reason.

**`sbatch` gets an environment with no Slurm variables in it.**
Every submission passes the `get_clean_environ` result to `sbatch`.
As a result, a driver inside a Slurm job submits the same jobs as one on a login node.
`get_clean_environ` says why.

## Conventions

- **After you change any Python, run `black`, then `pyright`, then `pytest`.**
  All three must be clean before you call the change done:
  `black` reports "left unchanged",
  `pyright` reports "0 errors",
  `pytest` passes.
  Run `black` first.
  `black` rewrites lines,
  so a type check before formatting
  can report positions that no longer exist.
  None of the three tools is advisory here.

  If `pyright` objects to a deliberate test double,
  say so with a `cast` and a comment.
  The comment says why the double is enough
  (see `as_collector` in `tests/test_swtop_tui.py`).
  Do not silence it with a bare `# type: ignore`.
  If `pyright` objects to something in `src/`, fix the annotation instead.
- **Deprecation warnings are errors in the test suite**
  (`filterwarnings` in `pyproject.toml`).
  They are how a dependency announces a break one release ahead.
  A warning nobody reads is a break discovered at the worst moment.
  Other warnings stay warnings.
- **Prose uses semantic line breaks.**
  Break at clause boundaries, not at a column limit.
  Start a new line after each sentence,
  and at punctuation that already separates clauses (`.` `:` `,`).
  Start one before a conjunction or a preposition that opens a new phrase.
  Never end a line mid-phrase,
  or on an article, a conjunction, a preposition or an auxiliary.
  Fixed-width wrapping produces those breaks.

  The result is a ragged right margin, and that margin is the point.
  A diff then shows only the clause that actually changed,
  instead of a whole reflowed paragraph.

  ```python
  # Wrong, wrapped at a column and broken mid-phrase:
  # The executor name prefixes every task id. It also names the
  # logger and the work dir.

  # Right, one clause per line:
  # The executor name prefixes every task id.
  # It also names the logger and the work dir.
  ```

  This convention governs `#` comment blocks, docstring prose,
  and every Markdown file in the repository:
  `README.md`, `docs/*.md`, and this file.
  Exempt: anything whose line structure is already meaningful.
  That covers code inside fences, Markdown tables, headings,
  and ASCII section banners (`# ---- name ----`).
- **[`terminology.md`](terminology.md) decides what a thing is called.**
  It binds prose and identifiers alike,
  so a name carries the same word the prose does.
  Where a concept has no row there, add one.
  Do not coin a second word for something the tables already name.
- **Each class releases what it holds in its own method.**
  That method is `close()` on `SlurmPilotExecutor` and `PilotWorker`,
  and `stop()` on a monitor thread.
  There is no shared base class for it.
