# Developer notes

[<- back to the main README](../README.md)

Notes for people working on `slurm-workflows` itself.
Everything here is about the code.
The guides the README indexes cover how to *use* the library.

`slurm-workflows` is a Python library (>=3.12)
of helpers for running work on Slurm HPC clusters.
It does two things, both covered by those guides:
the pilot-job executor, and the batch Bayesian optimizer built on it.

## Where documentation goes

User documentation lives under `docs/`,
organized by [Diataxis](https://diataxis.fr/) type.
The README is a landing page:
what the library is, requirements, install,
one minimal usage example, and the index of everything else.
The README links every document, so its tables are the one index.
This file does not keep a second copy of them.

```
docs/
  tutorials/        lessons: a learner runs a worked example end to end
  how-to-guides/    directions: a competent user solving one stated problem
  reference/        neutral description, one page per public module of src/
  explanation/      why the code is the way it is, for users
  developer-notes.md, how-to-run-tests.md    for contributors
```

For new user-facing documentation,
decide which of the four types it is before you decide where it goes.
Then add it to the matching table in the README.
Keep each document inside its type:
a tutorial that stops to explain links out to `explanation/` instead,
and reference describes rather than recommends.

The reference tree mirrors `src/slurm_workflows/`,
so you find a module and its reference page the same way.
A new public module needs a new page under `reference/`.

Nothing user-facing goes in `README.md` beyond that list.

### Docstrings, comments, and this file

Prose in the source is not a third documentation set.
Each kind of prose has one job:

| Where | Carries | Never carries |
| --- | --- | --- |
| Docstring | What a caller needs: what it does, its arguments, what comes back, what it raises | Why it was built this way, how it is implemented, anything a caller cannot act on |
| Comment | What is not obvious at that line, in a sentence or two | An argument for the design, or a paragraph the docs already carry |
| `docs/` | How to use the library, and what it does | |
| This file | Why the code is the way it is: the invariants, the trade-offs, the alternatives that were tried | |

A private helper's docstring is one line that says what it does.
Its reasoning belongs under [Invariants](#invariants),
where one reader looking for the design finds all of it.
That reasoning does not belong in a comment
that the next person to touch that function has to rediscover.

Where a design decision sits in both places,
the next editor changes only one of them.

## Commands

There is **no CI** in this repository, so nothing runs these for you.
The install and test commands are also at the top of [`how-to-run-tests.md`](how-to-run-tests.md).

```sh
pip install -ve .[test,dev]
black src tests examples    # format
pyright                     # type-check
pytest                      # test suite
```

`pyproject.toml` configures `black` and `pyright`.
`[tool.black]` pins `target-version` to the `requires-python` floor,
so formatting does not drift with whichever interpreter happens to run it.
`[tool.pyright]` sets the include paths and `pythonVersion`.
For this reason, run both bare.
**Do not pass paths to `pyright`**, or it ignores that configuration.

`pyproject.toml` defines two console entry points.
`slurm-pilot-worker` is internal:
generated sbatch scripts invoke it on the compute nodes,
and users never call it directly.
`swtop` is for users.
The [`swtop` reference](reference/swtop.md) describes it.

Deploy to clusters with `cpush`.
See `.cpush.json5` for the `rivanna` remote.

## Where things live

| Module | Holds |
| --- | --- |
| `slurm_pilot_executor.py` | `SlurmPilotExecutor` (the coordinator, on the login node) and `WorkerGroup` |
| `slurm_pilot_worker.py` | `PilotWorkerProcess` (runs inside Slurm jobs) and the `slurm-pilot-worker` CLI |
| `slurm_utils.py` | `sbatch` / `squeue` / `scancel` wrappers, `get_clean_environ()` |
| `optimize_space_botorch.py` | `OptimizationTask` and `OptimizeSpaceBotorch`, the botorch searches |
| `search_space.py` | `SearchSpace`, the `ParameterRange` types, and the unit cube mapping |
| `explore_space.py` | `ExplorationTask` and `ExploreSpaceSobolQMC`, Sobol' sweeps with no model behind them |
| `monitors.py` | Host and cgroup sampling, and the threads that publish it |
| `swtop.py` | The `swtop` monitor: `Collector`, the row builders, the text frames and the CLI |
| `swtop_tui.py` | The Textual app `swtop` runs in |
| `templates/` | Jinja templates and their loader |
| `utils.py` | `RemoteExecutionError`, id and logging helpers, `floor_power_of_two` and `index_width`, the progress-line formatters |

The coordinator and the workers never talk to each other directly.
They talk only through the `ds-service` server,
via `DsServiceClient` from the external `ds-service-client` package.
`swtop` uses `DsServiceClientAsync` instead,
the asyncio client of the same package and the same API,
for the reason given under Monitoring.
`DsServiceServer` (same package) can launch a local server process.
`SlurmPilotExecutor` always takes the address as its `server_address` argument.

The server and the client carry the same version.
`pyproject.toml` records the client floor,
and the server binary has to match it.
Install the latest `ds-service` release.

## Tools and libraries

`pyproject.toml` pins the versions and the extras.
This table says what each one is here for.

| Dependency | Used by | For |
| --- | --- | --- |
| `ds-service-client` | executor, worker, `swtop` | The queue server's client and its `DsServiceServer` launcher. The one channel between coordinator and workers. It also provides `task_search_id`, which is how `swtop` lists the tasks on a server. |
| `cloudpickle` | `slurm_pilot_executor`, `slurm_pilot_worker` | Serializing functions, arguments and return values, so a locally defined function can cross to a compute node. |
| `jinja2` | `templates/` | Rendering the worker shell script and its sbatch wrapper. |
| `json5` | `templates/` | Parsing the `{#- name: ... -#}` headers of the multi-template files. |
| `scipy` (>=1.15) | `explore_space` | `stats.qmc.Sobol` for the exploration design. The floor is for the `rng=` argument. |
| `textual` | `swtop_tui` | The `swtop` terminal UI. |
| `click` | `swtop`, `slurm_pilot_worker` | Both console entry points. |
| `psutil` | `monitors` | Host and process sampling. |
| `platformdirs` | `slurm_pilot_executor` | Locating the per-user cache dir a run's `work_dir` defaults into. |
| `typeguard` (>=3) | `slurm_pilot_executor` | `@typechecked` on the public surface. |
| `numpy` | (transitive use) | Arrays behind the search spaces and results. |
| `botorch` | `optimize_space_botorch` | The Gaussian process fit and the acquisition optimization, and `torch` underneath it. **Optional**, behind the `botorch` extra, and imported lazily so `import slurm_workflows` works without it. |

Development tooling, behind the `dev` and `test` extras:

| Tool | Extra | Role |
| --- | --- | --- |
| `pytest` | `test` | The suite. See [`how-to-run-tests.md`](how-to-run-tests.md). |
| `botorch` | `test` | So the optimizer tests run rather than skip. |
| `black` | `dev` | Formatting. Configured in `pyproject.toml`; run bare. |
| `pyright` | `dev` | Type checking. Configured in `pyproject.toml`; run bare. |
| `setuptools_scm` | build | Deriving the version from git tags, with a `1.0.0-dev` fallback. |
| `cpush` | external | Deploying to clusters. See `.cpush.json5`. |

There is no CI, and no linter beyond `pyright`.
The three commands under [Commands](#commands) are the whole gate.

## Invariants

Things that are easy to break and quiet when broken.

### Task flow

`as_completed` and `wait` poll `task_get_status`
with every still-pending id in one batched call.

**Status and output are separate RPCs.**
`task_get_status` returns only states,
so each finished task then needs its own `task_get_output`.

**`_as_completed` drops a task that can never run, not the batch.**
`_starved_tasks` and `_stranded_tasks` return the subset they object to,
rather than raising, because the answer is always a subset.
A queue nobody scaled says nothing about the queues that were.
Jobs in one group that reach their walltime
say nothing about a task on another group's queue.

A driver that abandons the rest of `pending`
loses results the server already has, silently under `RAISE_NEVER`.
Such a driver also breaks what `RAISE_AFTER_COMPLETED` promises.
The failure count in the deferred exception counts *tasks* for the same reason:
one message covers every task on a dead queue.

**Both drivers record what came back, even when the batch failed.**
Both drivers wait with `RAISE_AFTER_COMPLETED` and then record.
On the failure path they record what returned before re-raising.
A sweep of a few thousand points must not lose all of them to one,
and `save()` is what the next run reads.

**The driver warns about every failure, whatever `RaiseOnError` says.**
The warning is the part a caller cannot switch off,
because `RAISE_NEVER` otherwise loses a failure entirely.
`task.output` is the only other record,
and nothing forces a caller to read it.
The warning goes to stderr, so it does not land in a caller's stdout.

**A wait publishes its progress, but it does not draw it.**
`wait` and `as_completed` write the `progress_display` key
and append to `progress:<uuid4>` as tasks return,
and `swtop` is what turns that into a bar.
The driver prints nothing,
so a run under `nohup` leaves no progress bar in its output file.
A run that someone watches from another shell shows one.
The driver appends the count at most once a second,
so the cost does not grow with the batch.
The driver appends the final count even when the wait raises,
since the exception says nothing about how far it got.

**Only `wait` can defer.**
`as_completed` yields results as they arrive,
so there is no point at which it finished but the caller did not.
For this reason, `RAISE_AFTER_COMPLETED` collapses to `RAISE_ON_FIRST_ERROR` there.
`wait` therefore drives `_as_completed` itself,
and does not go through `as_completed`.
A route through `as_completed` rewrites the policy.

**The poll loop must name every `TaskState` explicitly.**
Its `else` branch means "keep waiting".
A state that falls through it therefore waits forever,
and the loop never reports that the task cannot finish.
ds-service later added `Canceled`, and that new state exposed this.

**`task_done` is per worker.**
The worker passes its own `worker_id`,
and the server refuses the call from any other worker.
The id given to `task_done`
has to be the one that claimed the task in `task_get`.

**An empty queue is `NoTaskAvailable`, not `TimeoutError`.**
`task_get` answers immediately when no queue has work,
and the worker sleeps and retries on that alone.
A `TimeoutError` there means an unreachable server
and must stay distinguishable.

**One executor per `ds-service` server.**
A server's queues, its `worker_job_info:`, `worker_process_info:`,
`task_name:` and `actor_class_args:` keys and its monitor counters
are one flat namespace with no executor in it.
For this reason, the design assumes a server belongs to a single executor.
Two executors on one server share queues by group name
and overwrite each other's actor arguments.

Nothing enforces this rule, because the executor cannot see another one.
That blindness is also why `_starved_tasks` and `_stranded_tasks`
refuse a queue served by jobs this executor did not start.
Task ids and worker names are still executor-prefixed,
because a *cluster* holds many runs even when a server holds one.

**`submit` sets `priority` to a *negated* wall clock.**
ds-service dispatches the highest priority first,
so a timestamp that rises with time serves the newest task first
and leaves the oldest until last.
The result is a queue that runs backwards and never says so.
A wall clock rather than `perf_counter` costs nothing
and keeps two processes' tasks comparable.
The one-executor-per-server invariant says two processes never share a server.
But a stale `Task` from a restarted driver still produces that case.

**Actor constructor arguments travel through the key value store.**
`define_worker` cloudpickles `actor_class_args` and `actor_class_kwargs`
into `actor_class_args:<group>` and `actor_class_kwargs:<group>`,
and `PilotWorkerProcess.__init__` reads them back under the same names.
The two sides agree by convention alone,
so the key format is part of the contract.
Change it in one place, and workers silently construct actors
with default arguments.

`define_worker` writes a key only when the caller gives a value.
For this reason, the worker treats `KeyError` as "none were passed"
rather than an error.
The `WorkerGroup` deliberately does not keep the values,
so a redefinition check never sees them.
The store is the only copy, and the last `define_worker` call wins.
The key holds the group name and not the executor's,
which is safe only because a server belongs to one executor.

**The executor's name is its identity.**
The name prefixes task ids, worker job names, script file names and the log,
and it keys the executor's logger.
Two live executors that share a name collide on all of those.
Their log lines land in both work dirs,
even when each executor has a server to itself.
`SlurmPilotExecutor` validates the name
(`[A-Za-z][A-Za-z0-9_-]*`, at least 3 characters).
The characters that are safe in a Slurm job name,
a directory name and a task id
are the intersection of three sets, not one.

**A run publishes itself in two halves, one key each.**
`SlurmPilotExecutor._add_worker` writes `worker_job_info:<worker-name>`
as soon as `sbatch` returns, so a queued job is visible before it runs,
and `PilotWorkerProcess.__init__` writes `worker_process_info:<worker-id>`
when the process starts.
`swtop` shows them as two blocks, and the difference between them
marks a job that is still queued.
Nothing ever updates either key, which is what makes both cacheable.

**Workers publish their identity at startup, as one key.**
`PilotWorkerProcess.__init__` writes `worker_process_info:<worker-id>`,
a JSON object, before it builds the actor.
A worker that dies in its actor's constructor
therefore still records which job and node it died on.
One key and not five, because `swtop` caches what it reads.
A reader that lands between two writes otherwise remembers
a worker whose host it never learned.

`WORKER_PROCESS_INFO_PREFIX` lives in `slurm_pilot_worker.py`
and `WORKER_JOB_INFO_PREFIX` in `slurm_pilot_executor.py`.
`swtop.py` imports both, so the writers and the reader cannot drift apart.
Nothing deletes the key:
the store is in memory and dies with the server,
which is the only cleanup there is.

**Anything `__init__` starts, a failed `__init__` has to stop.**
The monitors and the client are live before the worker builds the actor.
An actor constructor that raises means nothing ever calls `close()`.
The worker therefore stops its own monitors and closes its own channel
before it re-raises.

**Task names are UTF-8 in the store, not pickles.**
`set_task_name` writes `task_name:<task_id>` as encoded text,
unlike the actor arguments beside it.
The reason is that a name is a string,
and something other than this library has to read it.
`Task.task_name` is read-only for the same reason.
The store holds the other copy,
and an assignment to the attribute renames the task in this process alone.

The template inlines `setup_script` verbatim
into the generated worker script (`{{ setup_script }}`).
Nothing validates it.

### Logging

**The executor's logger carries the executor's name**
(`slurm_workflows.executor.<name>`), and does not propagate.
A name shared between executors collects one `FileHandler` per executor.
Every line then lands in every work dir opened in this process,
so the first executor's log fills up with the second's records.
`close()` removes and closes the handler.
The logger itself stays in the logging registry, inert.
For this reason, a test that reuses an executor name
inherits whatever handlers the previous one left on it.

### Templates (`templates/`)

`render_template` renders the sbatch and worker shell scripts
from Jinja2 templates in a **custom multi-template-per-file format**.
Each `.jinja` file holds several named templates,
each one under a `{#- name: "..." -#}` JSON5 header.
`templates/__init__.py` parses those headers.
A template address is `"<file_prefix>:<name>"`,
for example `"slurm_pilot:worker_script"`.

`render_template` carries `@overload` signatures
that document each template's required keyword arguments.
**Keep those overloads in sync when you change a template variable.**
The environment uses `StrictUndefined`, so a missing variable is a hard error.

`{#-` is also how a template body ends,
so a body cannot contain a whitespace-trimming Jinja comment.
The parser reads such a comment as the header of the next template.
Use `{#` without the dash inside a body.

### Search spaces (`search_space.py`)

**Never import torch or botorch here.**
This rule is the whole point of the split.
A search space is arithmetic on one value at a time,
so you can build and test one where the optimizer cannot be installed.
`tests/test_search_space.py` therefore runs without the `importorskip`
that skips every botorch test.
`optimize_space_botorch` imports only what it uses of it
and re-exports nothing.
Every importer takes a range from `search_space` alone.

**`to_unit` and `to_params` agree by the order of the space.**
A `SearchSpace` is an ordered mapping in practice,
and a unit point is a bare list of coordinates.
The column order is therefore the mapping's own iteration order.
Two spaces that hold the same ranges in a different order
are different spaces to a model fit on one of them.

### Sobol' exploration (`explore_space.py`)

**scipy's Sobol', not botorch's.**
`ExploreSpaceSobolQMC` draws with `scipy.stats.qmc.Sobol`,
so a sweep needs neither torch nor botorch,
and `tests/test_explore_space.py` runs without them.
`rng=` is the seed argument (`seed=` is the older spelling),
which is what the `scipy>=1.15` floor in `pyproject.toml` is for.

**`_resolve` floors the count to a power of two, and `design` draws with `random_base2`.**
A Sobol' sequence is only balanced on a power-of-two prefix,
and scipy warns whenever a caller asks for anything else.
Since `_resolve` floors the count anyway,
a request in scipy's own terms is the same draw without the warning.

**`run` submits every task before it waits for any of them.**
This order is what "simultaneously" means here:
one `submit` loop over every task's design, then a single `wait`.
A submit and a wait per task leaves the pool idle
whenever a small sweep finishes ahead of a large one.
That order also serializes tasks that name different queues,
even though nothing makes them wait for each other.

**`ExploreSpaceSobolQMC` validates a task in its constructor, not when the task runs.**
`_resolve` reports an empty space, a shadowed parameter or a missing point count
before anything reaches the cluster.
`_resolve` returns a copy with the point count and seed filled in,
so `self.tasks` says what will actually run.
The caller's own dataclass stays as they wrote it.

**`ExploreSpaceSobolQMC` shares the shape of `OptimizeSpaceBotorch`, not its code.**
Both classes submit a batch, wait with `RAISE_AFTER_COMPLETED`,
record what came back and report the best, in their own code.
The most error-prone part sits in one place.
`utils.objective_value` is the one place that checks an objective's result,
so the four rejection messages cannot drift apart.
The rest still can.
A fix to one class belongs in the other.

**`explore_space` owns the results file format.**
`load_results` reads what both `save` methods write,
and `SavedResults` says what a file holds.
The optimizer imports the reader rather than reimplementing it,
which is what keeps "the shape the explorer writes" true.
`unit_points` is deliberately not in the file.
Only the space can place a point in the unit cube,
and a stored copy can come from a different space.

### Batch Bayesian optimization (`optimize_space_botorch.py`)

**`OptimizeSpaceBotorch` never explores.**
You construct an `OptimizeSpaceBotorch` from results files,
and it fails if a task has no observations in them.
That dependence on files is what makes a search resumable.
The state that has to survive a walltime limit is a file, not an object.
`save` writes only what its own run measured,
so the files concatenate without double counting.

**A round is two batches, not two per task.**
`OptimizeSpaceBotorch` submits every active task's fit before it waits for any,
then every active task's proposed points.
Tasks therefore advance in step and drop out independently,
each against its own `patience`, floor and ceiling.

- **`fit_and_propose` fits the model to `-f`.**
  botorch maximizes and this library minimizes,
  so every acquisition value is in that negated space too.
  `qLogNoisyExpectedImprovement` takes no `best_f`:
  it reads its incumbent off the posterior at `X_baseline`,
  which is the same negated space again.
  Get the sign backwards and the search quietly walks uphill
  instead of failing.
- **`unit_points` holds the point actually evaluated**,
  re-standardized *after* rounding, never the continuous proposal.
  Otherwise the fit tells the GP about a location the objective never ran at.
- **The fit runs on a worker, not on the driver.**
  `_fit_and_propose` submits `fit_and_propose` to `optimizer_queue`
  as one task per round, the fit and the acquisition together.
  A fitted GP shipped back to the driver costs more than the fit did.
  Keep it a module-level function that takes and returns plain Python.
  Then cloudpickle sends it by reference,
  and no torch object has to survive a hop between hosts.
  Its workers need botorch.
  The workers of `objective_queue` do not.
- **The four acquisition knobs belong to the task, not to the process.**
  `num_restarts`, `raw_samples`, `mc_samples` and `acqf_timeout_s`
  are `OptimizationTask` fields with literal defaults,
  passed to every `fit_and_propose` task.
  A value read inside `fit_and_propose` is the *worker's*,
  and it ignores how the caller configured the search.
  Tests assert them by constructing with them
  (`make_task(acqf_timeout_s=...)`) or against `opt.tasks[i].<knob>`,
  never against a literal.
- **The stall counter runs from round 1.
  `min_search_iterations` gates the stop, not the counting.**
  Report the gap to the stop as `max(patience - stalled, min_search_iterations - iteration)`.
  A bare `stalled`/`patience` ratio runs past its own denominator
  whenever the floor outlasts the streak, which the defaults do.
- **One acquisition, one `optimize_acqf` call per round**,
  for the whole batch.
  `qLogNoisyExpectedImprovement` takes `X_baseline`,
  every point measured so far, rather than a `best_f` scalar.
  That argument has to be the `train_x` from this round's fit,
  not a stale copy.
- **Ask for the batch jointly, never `sequential=True`.**
  The sequential path is the usual advice for large batches,
  and it is wrong here.
  That path ran 10-15x *slower* on a low-dimensional space,
  because the greedy path pays
  the restart cost once per point instead of once per batch.
- **Ranges clamp in `unstandardize`**,
  because `optimize_acqf` can return a point slightly outside the bounds.
- **Never import this module eagerly from the package `__init__.py`.**
  `OptimizeSpaceBotorch` and `OptimizationTask` are importable
  from the package root, but through the `__getattr__` there.
  That `__getattr__` imports this module on first use,
  so `import slurm_workflows` still works without botorch installed.
  An import at the top of `__init__.py`
  makes botorch a hard dependency of the whole package.
- The module calls `optimize_acqf`, `fit_gpytorch_mll`,
  `qLogNoisyExpectedImprovement` and `fit_and_propose`
  through module globals.
  The tests monkeypatch those to assert what the code asked for,
  without paying for a real acquisition optimization.
  That works because `LocalExecutor` runs the submitted task inline,
  in the test's own process.
  The patch reaches the fit only for as long as that stays true.
- **`test_search_moves_toward_the_minimum` asserts the *median* search point**,
  not the max and not `best_point()`.
  Neither of those works.
  qLogNEI explores away from the incumbent,
  so the max hits 1.0 on correct runs.
  Exploration alone lands near the minimum,
  so `best_point()` passes even with the sign flipped.
  [`how-to-run-tests.md`](how-to-run-tests.md) carries the measured margins.

### Monitoring (`monitors.py`, `swtop.py`)

**One worker per subject samples, and a counter decides which.**
`counter_get_next_value` hands out distinct, gap-free values.
The worker told 1 for `host_monitor:<hostname>` takes the node,
and the one told 1 for `slurm_job_monitor:<job-id>` takes the job.
No lock, no designated rank, and no need for the workers to know each other.
Nothing hands a subject back when that worker dies:
the series stops, and `swtop` marks it stale.
A re-election needs a heartbeat and a lease, and this design has neither.

**Sampling threads are daemons that swallow their errors.**
A monitor must not hold open a worker that Slurm kills at its walltime,
and a failed sample must not end the series.
A node briefly unreachable is the common case, and a gap beats a stop.
`close()` stops them before closing the client whose channel they use.

**CPU is a rate, taken as the difference of two totals.**
The kernel reports CPU as microseconds that only rise,
so `CgroupSampler` keeps the previous reading.
The first sample of a run necessarily reports 0 cores.
`CgroupSampler` prefers the cgroup's own accounting to a sum over processes,
because the cgroup covers every process and thread Slurm put in the job.
That includes ones the worker never started.

**`swtop` can only show what an RPC can answer.**
`task_get_count_by_state` covers every task,
and `task_search_id` enumerates them.
Nothing enumerates workers, hosts or jobs,
so `swtop` builds those tables by a key space search
for keys the workers and the monitors publish.
`swtop` cannot list a worker that did not publish its identity.
That limit belongs to the server, and this library does not work around it.

**`swtop` reads only the tail of a series.**
`time_series_get` with no bounds returns every point ever appended,
which over a day-long run is most of the memory the server holds.
`swtop` asks for the last minute,
and calls a subject with nothing there stale.

**`Collector` reads an identity once.**
`Collector` caches every worker's fields and every task's name,
because nothing ever changes either after the first write.
Without the cache, a 400-worker pool costs 400 reads every 2 seconds,
plus one for every named task.
`Collector` does not cache a name that is not there yet.
`set_task_name` runs just after `submit`,
so a task polled between the two
otherwise stays `-` for the rest of the run.

**`swtop` draws a failed poll, and does not raise it.**
A monitor that exits when the server blinks
takes the screen down with it.
In the UI the tables stay as they were,
because the last good reading beats a blank screen
while a server restarts.

**`swtop` reads with the asyncio client.**
A poll is a handful of key searches plus a read per worker,
per named task and per monitored series.
One after another, that is a round trip apiece,
and a few hundred workers do not fit in a two-second interval.
`Collector` issues each group with `asyncio.gather`,
so a poll costs about one round trip however wide the pool is.
The cache means `Collector` reads only what is new.

**The UI polls in a worker, never inline.**
An awaited poll cannot block the interface the way a blocking one can.
Such a poll still must not run inside a message handler or a timer tick.
`run_worker(..., exclusive=True)` gives the poll a worker of its own
and cancels the poll already in flight, RPCs and all.
A slow server therefore cannot pile up a poll per interval.
The UI applies the result on the event loop like any other update,
with no `call_from_thread` in the way.

**`sync_table` updates a table in place, and never rebuilds it.**
`sync_table` adds, updates and removes rows by key,
which is a worker id, a hostname or a task id.
`DataTable.clear()` throws away the scroll position and the cursor,
which a 3600-worker pool needs to keep.
Those keys come from the row builders in `swtop.py`.

**Both displays read the same row builders.**
`worker_job_rows`, `worker_rows`, `host_rows`, `job_rows` and `task_rows`
are the one definition of what each block shows.
The text frames and the UI differ only in how they draw them.

### Slurm interaction (`slurm_utils.py`)

`is_batch_worker=False` wraps the worker script in `srun`
and passes `--output <work_dir>/<name>-%j-%t.out`.
That is why the `worker_sbatch_script` template takes `name` and `work_dir`.
[`reference/executor.md`](reference/executor.md#logs) documents for users
which file a worker's log ends up in.
This section says why the shell decides it rather than Python.

**The generated script drops `--output` for a job of exactly one task**,
which then writes to the batch job's own output file.
A per-task file only duplicates it.
The job decides at run time, in the shell,
because Python cannot know the answer when it renders the script.

`SLURM_NTASKS` is the number of tasks in the job
whenever the submission gave `--ntasks` or any `--ntasks-per-*` option.
`SLURM_NTASKS` settles the question alone.
Do not let anything else override it.

Slurm leaves it unset only when the submission asked for no task count.
That case is exactly when one task per node is the default,
so `SLURM_JOB_NUM_NODES` stands in there.
The count is per *job*, not per node:
`--nodes=4 --ntasks-per-node=1` is four tasks
and keeps its per-task files.

**The worker process must not redirect `sys.stdout` or `sys.stderr`.**
Slurm writes those files itself via `--output`,
and `logging.basicConfig` leaves the streams on the inherited handles.
A second redirect leaves the Slurm-written files empty.

**`submit_sbatch_job` searches `sbatch`'s stdout for the job id, and does not match at the start.**
A site that prints a banner or a warning there
otherwise turns a successful submission into a parse failure.

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
  Neither tool is advisory here.

  If `pyright` objects to a deliberate test double,
  say so with a `cast` and a comment that explains why the double is enough
  (see `as_executor` in `tests/test_optimize_space_botorch.py`).
  Do not silence it with a bare `# type: ignore`.
  If `pyright` objects to something in `src/`, fix the annotation instead.
  The `SearchSpace = Mapping[...]` alias exists because `dict[...]`
  is invariant and made a correct call fail to type-check.
- **Deprecation warnings are errors in the test suite**
  (`filterwarnings` in `pyproject.toml`).
  They are how a dependency announces a break one release ahead,
  and a warning nobody reads is a break discovered at the worst moment.
  Other warnings stay warnings.
  This library deliberately hands botorch a constant objective in places,
  and botorch says so at runtime.
- **Prose uses semantic line breaks.**
  Break at clause boundaries, not at a column limit.
  Start a new line after each sentence,
  and at punctuation that already separates clauses (`.` `:` `,`).
  Start one before a conjunction or preposition that opens a new phrase.
  Never end a line mid-phrase,
  on an article, conjunction, preposition or auxiliary,
  which is what fixed-width wrapping produces.
  The result is a ragged right margin, and that margin is the point.
  A diff then shows only the clause that actually changed,
  instead of a whole reflowed paragraph.
  Keep lines under the usual limit as a ceiling, not a target.

  ```python
  # Wrong -- wrapped at a column, breaking mid-phrase:
  # The seed is the only thing that decides the design. The name is for
  # progress bars and error messages.

  # Right -- one clause per line:
  # The seed is the only thing that decides the design.
  # The name is for progress bars and error messages.
  ```

  This convention governs `#` comment blocks, docstring prose,
  and every Markdown file in the repository:
  `README.md`, `docs/*.md`, and this file.
  Exempt: anything whose line structure is already meaningful.
  That covers code inside fences, Markdown tables, headings,
  and ASCII section banners (`# ---- name ----`).
- Cleanup is per-class.
  There is no shared base class for it.
- `setuptools_scm` derives the version from git tags,
  with a fallback of `1.0.0-dev`.
