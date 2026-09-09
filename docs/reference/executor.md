# `SlurmPilotExecutor`

[<- back to the main README](../../README.md)

`slurm_workflows.slurm_pilot_executor`:
the coordinator that runs on the login node,
the `Task` handle it returns,
and the `RaiseOnError` policy that decides what a failure does.

Everything public is importable from the package root:

```python
from slurm_workflows import SlurmPilotExecutor, RaiseOnError, RemoteExecutionError
```

The botorch names, `OptimizeSpaceBotorch` and `OptimizationTask`,
import from there too,
but are resolved on first use rather than when the package is imported,
so `import slurm_workflows` still works without botorch installed.

## `SlurmPilotExecutor(name, server_address, work_dir=None)`

```python
from slurm_workflows import SlurmPilotExecutor

executor = SlurmPilotExecutor(name, server_address, work_dir=None)
```

`name` identifies the executor.
It prefixes every task id (`<name>.task.<n>`)
and every worker's Slurm job name (`<name>.worker.<group>.<index>`),
and it names the executor's log.
Two executors on one cluster must have two names:
a shared one collides on all three,
whether or not they are talking to the same server.
It must start with a letter
and hold only letters, digits, `_` and `-` (`[A-Za-z][A-Za-z0-9_-]*`),
and be at least 3 characters long; anything else raises `ValueError`.

`server_address` is the `host:port` of the `ds-service` server.
Each executor must have a server of its own.
A server holds one run's tasks, worker registrations and actor arguments,
and everything on it is taken to belong to the executor that is using it.
Two executors pointed at one server share a queue namespace:
same-named worker groups serve each other's tasks,
and same-named groups overwrite each other's actor arguments.

`work_dir` defaults to a timestamped directory under
`<platform cache dir>/slurm-workflows/<name>`
(`XDG_CACHE_HOME`-driven on Linux),
so one executor's runs sit together;
generated scripts and all logs land there.

| Method | What it does |
| --- | --- |
| `define_worker(name, sbatch_args, ...)` | Register a worker group. Submits nothing. The group name is also the queue name. Idempotent - redefining a group identically is a no-op, redefining it differently asserts. |
| `scale_workers(name, count)` | Submit or cancel pilot jobs so the group has `count` jobs. |
| `submit(queue, fn, *args, **kwargs) -> Task` | Enqueue one task and return a `Task` straight away. `queue` is a group name or a list of them; `fn` is a callable, or a method name (`str`) for actor workers. |
| `as_completed(tasks, desc, unit="task", raise_on_error=...)` | Yield tasks as their results arrive. `desc` and `unit` label the progress `swtop` draws. Raises `RuntimeError` rather than blocking forever on a task that can never finish. |
| `wait(tasks, desc, unit="task", raise_on_error=...)` | Same, but discards the iterator - block until all are done. |
| `set_task_name(task, name)` | Name a task, on the queue server as well as locally. |
| `stop()` | Cancel all pilot jobs, keep the executor usable. |
| `close()` | Cancel all pilot jobs and close the queue-server connection. |

It is also a context manager.
Leaving the block calls `close()`,
so every pilot job is cancelled and the executor is spent afterwards.
An exception raised inside the block still propagates:

```python
with SlurmPilotExecutor(name="demo", server_address=address) as executor:
    executor.define_worker(name="cpu", sbatch_args=[...])
    executor.scale_workers("cpu", 4)
    ...
# close() has run: every pilot job is cancelled
# and the queue connection is shut.
```

A whole run is five calls:

```python
with SlurmPilotExecutor("my-run", address) as executor:
    executor.define_worker(name="cpu", sbatch_args=SBATCH_ARGS, setup_script=SETUP)
    executor.scale_workers("cpu", 1)

    tasks = [executor.submit("cpu", square, i) for i in range(100)]
    executor.wait(tasks, desc="squaring")

results = [task.output for task in tasks]
```

`sbatch_args` are passed straight through to `sbatch`,
so any Slurm option works.
Tasks may be submitted before any worker exists:
they wait on the queue until something pulls them.

## `define_worker` options

| Argument | Default | Meaning |
| --- | --- | --- |
| `setup_script` | `""` | Shell snippet run on the compute node before the worker starts - the text, not a path. Must be a `str`; omit it (or pass `""`) if `/etc/profile` (always sourced) already gives workers the right environment. |
| `is_batch_worker` | `False` | See [One worker per job, or one per task](#one-worker-per-job-or-one-per-task). |
| `actor_class_name` | `None` | Fully qualified class name to instantiate once per worker. |
| `actor_class_args` | `None` | Positional arguments for that class's constructor. Only valid with `actor_class_name`. |
| `actor_class_kwargs` | `None` | Keyword arguments for that class's constructor. Only valid with `actor_class_name`. |
| `python_paths` | `None` | Extra paths prepended to the workers' `sys.path`. |
| `add_cwd_to_python_path` | `True` | Also add the coordinator's cwd. |
| `worker_exe` | `"slurm-pilot-worker"` | Worker entry point, if you've wrapped or renamed it. |

The actor arguments are cloudpickled
and put in the `ds-service` key value store,
under `actor_class_args:<name>` and `actor_class_kwargs:<name>`,
where `<name>` is the worker group's name.
Each worker reads them back at startup.
They must be picklable,
and anything they refer to has to be importable on the compute node,
exactly as for the actor class itself.
They are not part of the group's identity,
so redefining a group with different ones is allowed,
unlike differing `sbatch_args`.
Only the workers started after that call read the new values:
an actor is constructed once, when its worker starts.

For the task-side view of actors, see
[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md).

## One worker per job, or one per task

`is_batch_worker` controls how many worker processes each Slurm job starts:

| Setting | Script is run with | Workers per job |
| --- | --- | --- |
| `is_batch_worker=False` (default) | `srun` | one per Slurm task in the allocation |
| `is_batch_worker=True` | sourced directly | one, on the batch node |

With the default, `--nodes=4 --ntasks-per-node=2`
gives 8 worker processes from a single `scale_workers(..., 1)` call.
`is_batch_worker=True` gives a single process
that owns the whole worker allocation,
which is what a multi-node (MPI or UPC++) task needs.

## Watching a wait

`desc` is required, and `unit` names what is being counted.
Neither call prints a progress bar of its own:
they publish what they are working through to the queue server,
where [`swtop`](swtop.md) draws it.

Each call writes the key `progress_display`, a JSON object holding

| Field | Value |
| --- | --- |
| `progress_id` | A fresh UUID4, one per call |
| `desc` | The `desc` given to the call |
| `unit` | The `unit` given to the call |
| `total` | How many tasks were handed in |

and appends the count that have come back so far
to the time series `progress:<progress_id>`,
opening at 0 and closing at the number that returned.
The count is appended at most once a second while tasks arrive.

The key is overwritten by the next call,
so the server holds the display for the most recent wait,
and the series holds the history of each.

## Errors that end a wait

A task whose queues have no worker can never finish,
so `as_completed` and `wait` raise `RuntimeError` naming those queues
rather than blocking. They check this twice.

**Before waiting at all**, and without asking Slurm,
they require that `scale_workers` has been called
for at least one of each pending task's queues.
Queue names are not validated at `submit` time,
so this is where a mistyped queue name is reported.
The error is raised before any result is yielded.

**Then once a minute while blocked**,
they ask `squeue` whether each pending task's queues
still have a job on the cluster,
which covers an allocation that ended, jobs that were cancelled,
and jobs that died before draining their queue.
The first of these checks is a minute in, not immediate.
A `squeue` that cannot be reached leaves liveness unknown rather than dead,
and is logged and retried instead of ending the wait.

Both checks only know about workers **this executor** started.
An executor that submits to a queue served by pilot jobs
some other process launched will be refused.

Two more states end a wait, both read straight off the queue server:

- **the server does not know the task id** -
  `RuntimeError: Task ... is unknown to the task queue server`.
  In practice a `Task` built by hand,
  or one left over from a server that has since been restarted.
- **the task was cancelled** -
  `RuntimeError: Task ... was canceled on the task queue server`.
  Nothing in this library cancels a task,
  so this means somebody called `task_cancel` through the `ds-service`
  client directly.
  A cancelled task is never dispatched again.

For what to do about each, see
[How to troubleshoot a failing run](../how-to-guides/troubleshoot-a-failing-run.md).

## `Task`

`submit` returns a `Task` with `task_id`, `queue`, `priority`, `function`,
`input`, and `output`.
`output` is a sentinel until the task completes;
after that it holds the return value -
or a `RemoteExecutionError(error, error_id)` if the worker raised.
`wait` and `as_completed` are what fill it in.

`task_name` is a read-only property, `None` until
`executor.set_task_name(task, name)` is called.
That call stores the name on the queue server, under `task_name:<task_id>`,
as UTF-8 rather than a pickle, so anything reading the store can read it too,
and updates the `Task` to match.
Nothing in this library dispatches on the name;
it is read by whoever is looking at the queue,
which in practice means [`swtop`](swtop.md).
`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` call it themselves
for every task they submit.

`priority` is assigned by `submit` and orders the queue.
`ds-service` dispatches the highest value first,
and `submit` sets it from a negated wall clock,
so tasks on one queue are served **oldest first**.
It is recorded on the `Task` for inspection;
changing it there has no effect,
since the value the server orders by was sent when the task was enqueued.

## `RaiseOnError`

What `as_completed` and `wait` do about a task that fails.
A failure is any of: a task whose worker raised
(its `output` is a `RemoteExecutionError`),
a task cancelled on the queue server,
a task the server does not know,
or a pending task whose queues have no pilot job left to run them.
Only the tasks that cannot finish are given up on;
the rest of the batch is still waited for.

| Value | Effect |
| --- | --- |
| `RAISE_ON_FIRST_ERROR` | The default. Stop at the first failure and raise `RuntimeError`. |
| `RAISE_AFTER_COMPLETED` | Wait for every task that can still finish, then raise once for all the failures together. `as_completed` treats this as `RAISE_ON_FIRST_ERROR`. |
| `RAISE_NEVER` | Report and return. |

**Every failure is warned about on stderr as it is met**, whichever value is used;
the value decides only whether an exception follows.
The warning carries the task id and,
for a worker that raised,
the `error_id` that appears beside the traceback in that worker's log.

With `RAISE_NEVER` the caller reads the outcome off the tasks:

```python
from slurm_workflows import RaiseOnError, RemoteExecutionError
from slurm_workflows.slurm_pilot_executor import NoOutput

executor.wait(tasks, desc="squaring", raise_on_error=RaiseOnError.RAISE_NEVER)

failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
never_ran = [t for t in tasks if t.output is NoOutput]
```

`output` stays `NoOutput` for a task that was cancelled,
is unknown to the server,
or was still pending when the last pilot job went away.

## What a run publishes

Inside a task, these environment variables are set:

- `PILOT_WORKER_NAME` - e.g. `demo.worker.cpu.0`
- `PILOT_WORKER_GROUP` - the group name
- `DS_SERVER_ADDRESS` - the queue server address
- plus the usual Slurm variables (`SLURM_JOB_ID`, ...)

**The executor publishes each pilot job as it submits it**,
under `worker_job_info:<worker-name>`, as a JSON object:

| Field | Value |
| --- | --- |
| `name` | The worker name, which is also the Slurm job name |
| `group` | The group whose queue it will serve |
| `slurm_job_id` | The job `sbatch` returned |
| `submit_time` | When it was submitted, an ISO 8601 timestamp with an offset |

**Each worker process publishes where it is running when it starts**,
under `worker_process_info:<worker-id>`,
where the worker id is `<worker-name>.<slurm-job-id>.<hostname>.<pid>`.
The value is a JSON object, not a pickle,
so anything can read it:

| Field | Value |
| --- | --- |
| `group` | The group whose queue it serves |
| `name` | The worker's name, which is its Slurm job name |
| `slurm_job_id` | The job it is running in |
| `hostname` | The compute node it landed on |
| `pid` | Its process id on that node |

The worker id is the handle the queue server hands out
(`task_get_worker_id` says which worker took a task),
so this is how you get from a task
to the process and node that ran it.
Nothing removes the key when a worker exits.

Workers also sample the node they run on and the Slurm job they belong to,
appending to `ds-service` time series every 5 seconds
(`host_free_memory:<hostname>`, `host_load_average:<hostname>`,
`host_dev_shm_used:<hostname>`, `host_tmp_used:<hostname>`,
`slurm_job_memory:<job-id>` and `slurm_job_cpu:<job-id>`).
One worker per node and one per job does this,
elected between them with the `host_monitor:<hostname>`
and `slurm_job_monitor:<job-id>` counters.
[`swtop`](swtop.md) displays the result.

Why a run is published in these two halves rather than one,
and why a key is written once and never updated, is in
[About what a run publishes](../explanation/about-what-a-run-publishes.md).

## Logs

Everything for a run lives under the executor's `work_dir`
(printed as `executor.work_dir`):

| File | Contents |
| --- | --- |
| `executor.log` | Worker submission and cancellation from the executor's side |
| `<worker-name>.sh`, `<worker-name>.sbatch` | The generated scripts |
| `<worker-name>-<jobid>-<task>.out` | One per worker process: setup-script trace, task-by-task progress, full tracebacks |
| `<worker-name>-<jobid>.out` | The batch job's own output - and the worker's log too, when the job is a single task |

`<worker-name>` is `<executor-name>.worker.<group>.<index>`,
which is also the Slurm job name, so `squeue` shows which run a job belongs to.
The work dir itself defaults to `<cache dir>/slurm-workflows/<executor-name>/<timestamp>`.

Slurm writes those files; the worker process does not redirect its own output.
Which of the two holds a worker's log depends on how the group was defined:

- **`is_batch_worker=False`** (the default) runs the worker under `srun`,
    which fans out over every task in the allocation.
    Each task gets `--output <work-dir>/<worker-name>-%j-%t.out`,
    so `<task>` is the task's rank - that file is the worker's log.
    `<worker-name>-<jobid>.out` then holds
    only what the batch script itself emitted,
    which in practice means `srun`'s own errors.

    The exception is a job of exactly one task -
    `--ntasks=1`, or `--nodes=1` with nothing else said about tasks.
    It keeps `srun` but drops the `--output`
    and writes to `<worker-name>-<jobid>.out` like a batch worker.
    The count is per *job*, not per node:
    `--nodes=4 --ntasks-per-node=1` is four tasks
    and still gets four per-task files.

    Which way a job went is recorded:
    the batch file opens with the task count the job decided on
    (`Num tasks: 4`), says so when it redirects,
    and traces the `srun` command it ran.
- **`is_batch_worker=True`** runs one worker directly on the batch node,
    with no `srun` and so no per-task file.
    Everything lands in `<worker-name>-<jobid>.out`.

The `error_id` inside a `RemoteExecutionError`
appears verbatim next to the traceback in the worker's log.
