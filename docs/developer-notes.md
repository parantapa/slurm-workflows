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
what the library is, install,
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
Keep each document inside its type:
a tutorial that stops to explain links out to `explanation/` instead,
and reference describes rather than recommends.

A reference page covers one thing a user reaches for.
The page is named after that class, that command or that subject,
rather than after the module it happens to live in.
`swtop.md` covers `swtop.py` and `swtop_tui.py` together.
What the worker and the monitors publish is documented
where a user meets it, rather than under its own module.
A new public class or command needs a page under `reference/`
and a row in the README's table.

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

Paths are relative to `src/slurm_workflows/`.

| Module | Holds |
| --- | --- |
| `__init__.py` | The public API, the one place users import from. Loads the botorch module lazily. |
| `slurm_pilot_executor.py` | The driver side, entry point `SlurmPilotExecutor`: job groups, submitting tasks, waiting on them, and `mapreduce`. Depends on `slurm_utils`, `templates/` and `utils`, and on `slurm_pilot_worker` for `current_actor`. |
| `slurm_pilot_worker.py` | The worker side, entry point the `slurm-pilot-worker` command that generated scripts run on compute nodes. Starts the monitors. |
| `slurm_utils.py` | Calls to the Slurm commands, and the environment `sbatch` runs in |
| `search_space.py` | Search spaces and the mapping to and from the unit cube. Imports no torch. |
| `explore_space.py` | Sobol' explorations with no model behind them, and the results file format the optimizer also reads |
| `optimize_space_botorch.py` | The botorch searches. Optional, behind the `botorch` extra. |
| `monitors.py` | Host and cgroup sampling, and the threads that publish it |
| `swtop.py` | `swtop`, entry point the `swtop` command: reading a run from the server, and the text display |
| `swtop_tui.py` | The Textual app `swtop` runs in |
| `templates/` | Jinja templates for the generated scripts, and their loader |
| `utils.py` | Helpers shared across modules: the remote error record, ids, the check on an objective's result, and formatting |

`tests/` holds the suite.
[`how-to-run-tests.md`](how-to-run-tests.md#layout) maps its files.
`examples/` holds the scripts the tutorials walk through.
`extra/` holds the README's banner image
and a FoxyProxy configuration for Rivanna.

The driver and the workers never talk to each other directly.
They talk only through the `ds-service` server,
via `DsServiceClient` from the external `ds-service-client` package.
`swtop` uses `DsServiceClientAsync` instead,
the asyncio client of the same package and the same API,
for the reason given under Monitoring.
`DsServiceServer` (same package) can launch a local server process.
`SlurmPilotExecutor` always takes the address as its `server_address` argument.

The server and the client carry the same version.
`pyproject.toml` records the client floor, `>=7.0.0`,
and not an exact version.
Install the `ds-service` release
with the same version as the installed client.

## Tools and libraries

`pyproject.toml` pins the versions and the extras.
This table says what each one is here for.

| Dependency | Used by | For |
| --- | --- | --- |
| `ds-service-client` | executor, worker, `monitors`, `swtop` | The `ds-service` server's client and its `DsServiceServer` launcher. The one channel between the driver and the workers. It also provides `task_search_id`, which is how `swtop` lists the tasks on a server. |
| `cloudpickle` | `slurm_pilot_executor`, `slurm_pilot_worker` | Serializing functions, arguments and return values, so a locally defined function can cross to a compute node. |
| `jinja2` | `templates/` | Rendering the worker shell script and its sbatch wrapper. |
| `json5` | `templates/` | Parsing the `{#- name: ... -#}` headers of the multi-template files. |
| `scipy` (>=1.15) | `explore_space` | `stats.qmc.Sobol` for the exploration design. The floor is for the `rng=` argument. |
| `textual` | `swtop_tui` | The `swtop` terminal UI. |
| `click` | `swtop`, `slurm_pilot_worker` | Both console entry points. |
| `psutil` | `monitors` | Host and process sampling. |
| `platformdirs` | `slurm_pilot_executor` | Locating the per-user cache dir a run's `work_dir` defaults into. |
| `typeguard` (>=3) | `slurm_pilot_executor` | `@typechecked` on the public surface. |
| `numpy` | none in `src/` | Declared in `pyproject.toml`, but no module imports it. The Sobol' design `scipy` returns is a numpy array, and `explore_space` turns each row into a list. |
| `botorch` | `optimize_space_botorch` | The Gaussian process fit and the acquisition optimization, and `torch` underneath it. **Optional**, behind the `botorch` extra, and imported lazily so `import slurm_workflows` works without it. |

Development tooling, behind the `dev` and `test` extras:

| Tool | Extra | Role |
| --- | --- | --- |
| `pytest` | `test` | The suite. See [`how-to-run-tests.md`](how-to-run-tests.md). |
| `botorch` | `test` | So the optimizer tests run rather than skip. |
| `black` | `dev` | Formatting. Configured in `pyproject.toml`. Run it bare. |
| `pyright` | `dev` | Type checking. Configured in `pyproject.toml`. Run it bare. |
| `setuptools_scm` | build | Deriving the version from git tags, with a `1.0.0-dev` fallback. |
| `cpush` | external | Deploying to clusters. See `.cpush.json5`. |

There is no linter beyond `pyright`.

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
Jobs in one job group that reach their time limit
say nothing about a task on another job group's queue.

A wait that abandons the rest of `pending`
loses results the server already has, silently under `RAISE_NEVER`.
Such a wait also breaks what `RAISE_AFTER_COMPLETED` promises.
The failure count in the deferred exception counts *tasks* for the same reason:
one message covers every task on a dead queue.

**Both space classes record what came back, even when the batch failed.**
`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch`
wait with `RAISE_AFTER_COMPLETED` and then record.
On the failure path they record what returned before re-raising.
An exploration of a few thousand points must not lose all of them to one,
and `save()` is what the next run reads.

**The executor warns about every failure, whatever `RaiseOnError` says.**
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
A wait appends the count at most once a second,
so the cost does not grow with the batch.
A wait appends the final count even when it raises,
since the exception says nothing about how far it got.

**Only `wait` can defer.**
`as_completed` yields results as they arrive,
so there is no point at which it finished but the caller did not.
For this reason, `RAISE_AFTER_COMPLETED` collapses to `RAISE_ON_FIRST_ERROR` there.
`wait` therefore drives `_as_completed` itself,
and does not go through `as_completed`.
A route through `as_completed` rewrites the policy.

**A worker marks a task that raised as `Failed`.**
It calls `task_done` with `failed=True`,
so the server fails every task that waits on it.
A task that the server failed for this reason never ran.
Its output is the plain text `Dependency failed (task_id=...)`,
not a cloudpickle,
and the poll loop checks for that prefix before it unpickles.

**`task_done` is per worker.**
The worker passes its own `worker_id`,
and the server refuses the call from any other worker.
The id given to `task_done`
has to be the one that claimed the task in `task_get`.

**One executor per `ds-service` server.**
A server's queues, its `pilot_job_info:`, `worker_info:`,
`task_name:` and `actor_class_args:` keys and its monitor counters
are one flat namespace with no executor in it.
For this reason, the design assumes a server belongs to a single executor.
Two executors on one server share queues by group name
and overwrite each other's actor arguments.

Nothing enforces this rule, because the executor cannot see another one.
That blindness is also why `_starved_tasks` and `_stranded_tasks`
refuse a queue served by jobs this executor did not start.
Task ids and worker ids are still executor-prefixed,
because a *cluster* holds many runs even when a server holds one.

**`submit` defaults `task_priority` to `0.0`.**
ds-service dispatches the highest priority first,
and tasks of equal priority on one queue oldest first.
The default therefore keeps submission order.
Item tasks and map tasks of `mapreduce` also use `0.0`.
A priority taken from a rising clock serves the newest task first
and leaves the oldest until last.

**The payload decides how the worker resolves a task's function.**
`main` looks a `str` up on the actor, and calls a callable as it is.
The check is the type of what `task.function` unpickles to,
never whether the job group has an actor.
A job group with an actor therefore still runs a plain callable,
which is what lets `mapreduce` submit its own task function there.
A `str` with no actor to find it on raises,
rather than failing later as a call on a string.

**The worker publishes its actor to the process, as `current_actor()`.**
A task the worker runs has no argument that carries the actor,
so `_mapreduce_task` reads it from the module.
One worker is one process and one actor, so a module global holds it.
`close()` clears it, and only when the actor it holds is this worker's own,
because a test builds two workers in one process.

**Actor constructor arguments travel through the map.**
`define_job_group` cloudpickles `actor_class_args` and `actor_class_kwargs`
into `actor_class_args:<group>` and `actor_class_kwargs:<group>`,
and `PilotWorker.__init__` reads them back under the same names.
The two sides agree by convention alone,
so the key format is part of the contract.
Change it in one place, and workers silently construct actors
with default arguments.

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
The characters that are safe in a Slurm job name,
a directory name, a logger name and a task id
are the intersection of four sets, not one.

**A run publishes itself in two halves, one key each.**
`SlurmPilotExecutor._add_job` writes `pilot_job_info:<job-name>`
as soon as `sbatch` returns, so a queued job is visible before it runs,
and `PilotWorker.__init__` writes `worker_info:<worker-id>`
when the process starts.
`swtop` shows them as two blocks, and the difference between them
marks a job that is still queued.
Nothing ever updates either key, which is what makes both cacheable.

**Workers publish their identity at startup, as one key.**
`PilotWorker.__init__` writes `worker_info:<worker-id>`,
a JSON object, before it builds the actor.
A worker that dies in its actor's constructor
therefore still records which job and node it died on.
One key and not five, because `swtop` caches what it reads.
A reader that lands between two writes otherwise remembers
a worker whose host it never learned.

`WORKER_INFO_PREFIX` lives in `slurm_pilot_worker.py`
and `PILOT_JOB_INFO_PREFIX` in `slurm_pilot_executor.py`.
`swtop.py` imports both, so the writers and the reader cannot drift apart.
Nothing deletes the key:
the map is in memory and dies with the server,
which is the only cleanup there is.

**Task names are UTF-8 in the map, not pickles.**
`set_task_name` writes `task_name:<task_id>` as encoded text,
unlike the actor arguments beside it.
The reason is that a name is a string,
and something other than this library has to read it.
`Task.task_name` is read-only for the same reason.
The map holds the other copy,
and an assignment to the attribute renames the task in this process alone.

The template inlines `setup_script` verbatim
into the generated worker script (`{{ setup_script }}`).
Nothing validates it.

### Mapreduce

**`mapreduce` puts every item task on the item queue
before it submits the first map task.**
This is the whole basis of the call,
and it is what lets a map task read `NoTaskAvailable` as "the work is done".
`task_get` raises it when no queue it polled has a *ready* task.
On its own that means everything is claimed,
not that nothing more arrives.
The ordering supplies the other half:
no map task can run before every item task exists,
and `mapreduce` adds nothing to the item queue afterward.

Break that ordering, by streaming the iterable or by topping the item queue up.
A fast map task then drains what is there and sees an empty item queue.
It returns a partial result that covers part of the input.
Nothing raises.
The call returns a plausible wrong answer.
That is why `mapreduce` reads the iterable out into a list first,
and why a test asserts on the order of the `task_add` calls.

**A mapreduce call gets an item queue of its own,
and no job group serves it.**
Workers poll their own job group's queue only,
so that call's own map tasks drain the item queue and nothing else does.
It also stays clear of `_starved_tasks` and `_stranded_tasks`.
Both look at the queues of the tasks handed to the wait,
never at the item queue.

The queue name carries a UUID token as well as a counter.
The counter restarts at 0 in a new process.
Without the token, a re-run of one executor name against a surviving server
collides with the item tasks the first run left behind.
`task_add` refuses a duplicate id,
so that collision fails the call with items already on the server.

**A map task resolves a method name once, not once per item.**
A worker builds its actor at startup and keeps it for the pilot job.
The bound method is therefore the same for every item the map task folds.
`reduce_fn` takes no method name at all,
because the driver folds the partial results with it
and there is no actor there.

**A map task builds a client of its own.**
A map task has no handle on the worker's client,
and that client belongs to the worker's own loop in any case.
`DsServiceClient()` reads `DS_SERVER_ADDRESS`,
and the task claims its items under `PILOT_WORKER_ID`.
`PilotWorker.__init__` puts both in the environment.
A test that drives that class gets them,
and one that calls the task function directly sets them itself.

A `with` block closes the client.
A worker runs many tasks over the life of its pilot job.
A leaked gRPC channel per task therefore accumulates for all of it.

**A map task marks its item task done after it folds the value in, not before,
and it records an empty output.**
`Finished` on the item queue therefore means "counted",
which is what a reader of the item queue expects.
The mapped value travels home inside the map task that computed it.
A second copy on the item task
holds the whole iterable on the server twice.

**The driver folds into a copy of `init`.**
Each map task already folds into a copy of its own,
the one its input deserialized into.
A `reduce_fn` that folds in place is therefore correct on a worker.
Without the copy it is not correct on the driver.
There it writes into the caller's own value,
and a second call starts from the answer of the first.

The copy is a cloudpickle round trip rather than `copy.deepcopy`.
The local fold and the remote folds then get the same kind of copy,
and an `init` that cannot travel fails at the call.

The copy does not remove the requirement that `init` be an identity.
Every map task folds it in once, and the call folds it in once more.

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
Each `.jinja` file holds one or more named templates,
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
so you can build and test one where you cannot install the optimizer.
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
so an exploration needs neither torch nor botorch,
and `tests/test_explore_space.py` runs without them.
`rng=` is the seed argument (`seed=` is the older spelling),
which is what the `scipy>=1.15` floor in `pyproject.toml` is for.

**`run` submits every task before it waits for any of them.**
This order is what "simultaneously" means here:
one `submit` loop over every study's design, then a single `wait`.
A submit and a wait per study leaves the pool idle
whenever a small study finishes ahead of a large one.
That order also serializes studies that name different queues,
even though nothing makes them wait for each other.

**`ExploreSpaceSobolQMC` validates a study in its constructor, not when the study runs.**
`_resolve` reports an empty space, a shadowed parameter or a missing point count
before anything reaches the cluster.
`_resolve` returns a copy with the point count and seed filled in,
so `self.studies` says what will actually run.
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
and it fails if a study has no observations in them.
That dependence on files is what makes a search resumable.
The state that has to survive a time limit is a file, not an object.
`save` writes only what its own run measured,
so the files concatenate without double counting.

**A round is two batches, not two per study.**
`OptimizeSpaceBotorch` submits every active study's fit before it waits for any,
then every active study's candidates.
The studies therefore advance in step and drop out independently,
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
  re-standardized *after* rounding, never the continuous candidate.
  Otherwise the fit tells the GP about a location the objective never ran at.
- **The fit runs on a worker, not on the driver.**
  `_fit_and_propose` submits `fit_and_propose` to `optimizer_queue`
  as one task per study per round, the fit and the acquisition together.
  A fitted GP shipped back to the driver costs more than the fit did.
  Keep it a module-level function that takes and returns plain Python.
  Then cloudpickle sends it by reference,
  and no torch object has to survive a hop between hosts.
  Its workers need botorch.
  The workers of `objective_queue` do not.
- **The four acquisition knobs belong to the study, not to the process.**
  `num_restarts`, `raw_samples`, `mc_samples` and `acqf_timeout_s`
  are `OptimizationStudy` fields with literal defaults,
  passed to every `fit_and_propose` task.
  A value read inside `fit_and_propose` is the *worker's*,
  and it ignores how the caller configured the search.
  Tests assert them by constructing with them
  (`make_opt(..., acqf_timeout_s=...)`) or against `opt.studies[i].<knob>`,
  never against a literal.
- **The stall counter runs from round 1.
  `min_search_rounds` gates the stop, not the counting.**
  Report the gap to the stop as
  `max(patience - stalled, min_search_rounds - round_number)`.
  A bare `stalled`/`patience` ratio runs past its own denominator
  whenever the floor outlasts the streak, which the defaults do.
- **One acquisition, one `optimize_acqf` call per study per round**,
  for the whole batch.
  `qLogNoisyExpectedImprovement` takes `X_baseline`,
  every point measured so far, rather than a `best_f` scalar.
  That argument has to be the `train_x` from this round's fit,
  not a stale copy.
- **Never import this module eagerly from the package `__init__.py`.**
  `OptimizeSpaceBotorch` and `OptimizationStudy` are importable
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
A monitor must not hold open a worker that Slurm kills at its time limit,
and a failed sample must not end the series.
A node briefly unreachable is the common case, and a gap beats a stop.
`close()` stops them before closing the client whose channel they use.

**CPU is a rate, taken as the difference of two totals.**
The kernel reports CPU as microseconds that only rise,
so `CgroupSampler` keeps the previous reading.
The first sample of a run necessarily reports 0 cores.

**`swtop` can only show what an RPC can answer.**
`task_get_count_by_state` covers every task,
and `task_search_id` enumerates them.
Nothing enumerates workers, hosts or jobs,
so `swtop` builds those tables by searching the map
for the keys the workers and the monitors publish.
`swtop` cannot list a worker that did not publish its identity.
That limit belongs to the server, and this library does not work around it.

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
A display that exits when the server blinks
takes the screen down with it.
In the UI the tables stay as they were,
because the last good reading beats a blank screen
while a server restarts.

**`swtop` reads with the asyncio client.**
A poll is a handful of key searches plus a read per worker,
per named task and per monitored series.
One after another, that is a round trip apiece,
and a few hundred workers do not fit in a two-second interval.
`Collector` issues each set of reads with `asyncio.gather`,
so a poll costs about one round trip however wide the pool is.
The cache means `Collector` reads only what is new.

**`sync_table` updates a table in place, and never rebuilds it.**
`sync_table` adds, updates and removes rows by key,
which is a job name, a worker id, a hostname, a job id or a task id.
`DataTable.clear()` throws away the scroll position and the cursor,
which a 3600-worker pool needs to keep.
Those keys come from the row builders in `swtop.py`.

**Both displays read the same row builders.**
`pilot_job_rows`, `worker_rows`, `host_rows`, `job_rows` and `task_rows`
are the one definition of what each block shows.
The text frames and the UI differ only in how they draw them.

### Slurm interaction (`slurm_utils.py`)

`is_batch_worker=False` wraps the worker script in `srun`
and passes `--output <work_dir>/<name>-%j-%t.out`.
That is why the `worker_sbatch_script` template takes `name` and `work_dir`.
[`reference/what-a-run-publishes.md`](reference/what-a-run-publishes.md#logs) documents for users
which file a worker's log ends up in.
This section says why the shell decides it rather than Python.

**The generated script drops `--output` for a job of exactly one Slurm task**,
which then writes to the batch job's own output file.
A file per Slurm task only duplicates it.
The job decides at run time, in the shell,
because Python cannot know the answer when it renders the script.

`SLURM_NTASKS` is the number of Slurm tasks in the job
whenever the submission gave `--ntasks` or any `--ntasks-per-*` option.
`SLURM_NTASKS` settles the question alone.
Do not let anything else override it.

Slurm leaves it unset only when the submission asked for no Slurm task count.
That case is exactly when one Slurm task per node is the default,
so `SLURM_JOB_NUM_NODES` stands in there.
The count is per *job*, not per node:
`--nodes=4 --ntasks-per-node=1` is four Slurm tasks
and keeps a file for each of them.

**The worker must not redirect `sys.stdout` or `sys.stderr`.**
Slurm writes those files itself via `--output`,
and `logging.basicConfig` leaves the streams on the inherited handles.
A second redirect leaves the Slurm-written files empty.

**`submit_sbatch_job` searches `sbatch`'s stdout for the job id, and does not match at the start.**
A site that prints a banner or a warning there
otherwise turns a successful submission into a parse failure.

**`sbatch` gets an environment with no Slurm variables in it.**
`get_clean_environ` drops `SLURM_`, `SLURMD_`, `PMI_` and `SRUN_`.
The driver runs inside a Slurm job as well as on a login node.
`sbatch` reads several `SLURM_*` variables as defaults for the job it submits.
If those variables reach `sbatch`,
every pilot job takes the driver's own node count and Slurm task count.
Every submission passes the `get_clean_environ` result to `sbatch`,
because a login node carries none of those variables anyway.
`get_clean_environ` builds that environment once per process (`@cache`),
so a later change to `os.environ` does not reach `sbatch`.

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
  say so with a `cast` and a comment.
  The comment says why the double is enough
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
- **[`terminology.md`](terminology.md) decides what a thing is called.**
  It binds prose and identifiers alike,
  so a name carries the same word the prose does.
  Where a concept has no row there, add one
  rather than coin a second word for something the tables already name.
- Cleanup is per-class.
  There is no shared base class for it.
