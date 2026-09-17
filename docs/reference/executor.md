# `SlurmPilotExecutor`

[<- back to the main README](../../README.md)

`slurm_workflows.slurm_pilot_executor`:
the executor, the `Task` handle it returns,
and the `RaiseOnError` policy that decides what a failure does.
[`mapreduce`](mapreduce.md)
and [what a run publishes](what-a-run-publishes.md)
have pages of their own.
The driver runs on a login node, or inside a Slurm job.

Everything public is importable from the package root,
except `NoOutput`:

```python
from slurm_workflows import SlurmPilotExecutor, RaiseOnError, RemoteExecutionError
```

The botorch names, `OptimizeSpaceBotorch` and `OptimizationStudy`,
import from there too.
The package resolves them on first use rather than at import time,
so `import slurm_workflows` still works without botorch installed.

## `SlurmPilotExecutor(name, server_address, work_dir=None)`

```python
from slurm_workflows import SlurmPilotExecutor

executor = SlurmPilotExecutor(name, server_address, work_dir=None)
```

`name` identifies the executor.
It prefixes every task id (`<name>.task.<n>`)
and every pilot job's Slurm job name (`<name>.job.<group>.<index>`),
and it names the executor's log.
Two executors on one cluster must have two names.
A shared one collides on all three,
whether or not they talk to the same server.

It must start with a letter
and hold only letters, digits, `_` and `-` (`[A-Za-z][A-Za-z0-9_-]*`).
It must also be at least 3 characters long.
Anything else raises `ValueError`.

`server_address` is the `host:port` of the `ds-service` server.
Each executor must have a server of its own.
A server holds one run's tasks, worker registrations and actor arguments.
The library assumes everything on it belongs to the executor that uses it.
Two executors pointed at one server share a queue namespace.
Same-named job groups serve each other's tasks,
and they overwrite each other's actor arguments.

`work_dir` defaults to a timestamped directory under
`<platform cache dir>/slurm-workflows/<name>`
(`XDG_CACHE_HOME`-driven on Linux),
so one executor's runs sit together.
Generated scripts and all logs land there.

## Methods

| Method | What it does |
| --- | --- |
| `define_job_group(name, sbatch_args, ...)` | Register a job group. Submits nothing. The job group name is also the queue name. A second identical definition does nothing. A definition that differs raises `AssertionError`. |
| `scale_jobs(name, count)` | Submit or cancel pilot jobs so the job group has `count` jobs. |
| `submit(queue, fn, *args, **kwargs) -> Task` | Enqueue one task and return a `Task` straight away. `queue` is a job group name or a list of them. `fn` is a callable, or a method name (`str`) for actor workers. |
| [`mapreduce(desc, queue, ...)`](mapreduce.md) | Map an iterable across the pool and fold the results into one value. Blocks. `init` must be the identity of `reduce_fn`. |
| `as_completed(tasks, desc, unit="task", raise_on_error=...)` | Yield tasks as their results arrive. `desc` and `unit` label the progress `swtop` draws. Raises `RuntimeError` rather than blocking forever on a task that can never finish. |
| `wait(tasks, desc, unit="task", raise_on_error=...)` | Same, but discards the iterator. Blocks until all are done. |
| `set_task_name(task, name)` | Name a task, on the server as well as locally. |
| `stop()` | Cancel all pilot jobs, keep the executor usable. |
| `close()` | Cancel all pilot jobs and close the server connection. |

It is also a context manager.
On leaving the block, Python calls `close()`.
That call cancels every pilot job, and the executor is spent afterward.
An exception raised inside the block still propagates:

```python
with SlurmPilotExecutor(name="demo", server_address=address) as executor:
    executor.define_job_group(name="cpu", sbatch_args=[...])
    executor.scale_jobs("cpu", 4)
    ...
# close() has run: every pilot job is canceled
# and the server connection is shut.
```

A whole run is five calls:

```python
with SlurmPilotExecutor("my-run", address) as executor:
    executor.define_job_group(name="cpu", sbatch_args=SBATCH_ARGS, setup_script=SETUP)
    executor.scale_jobs("cpu", 1)

    tasks = [executor.submit("cpu", square, i) for i in range(100)]
    executor.wait(tasks, desc="squaring")

results = [task.output for task in tasks]
```

The executor passes `sbatch_args` straight through to `sbatch`,
so any Slurm option works.
The executor accepts tasks before any worker exists:
they wait on the queue until a worker claims them.

## `define_job_group` options

| Argument | Default | Meaning |
| --- | --- | --- |
| `setup_script` | `""` | Shell snippet run on the compute node before the worker starts. The text, not a path. Must be a `str`. An omitted value (or `""`) leaves each worker with what `/etc/profile`, which is always sourced, gives it. |
| `worker_exe` | `"slurm-pilot-worker"` | Worker entry point, for a wrapped or renamed one. |
| `is_batch_worker` | `False` | See [One worker per pilot job, or one per Slurm task](#one-worker-per-pilot-job-or-one-per-slurm-task). |
| `actor_class_name` | `None` | Fully qualified class name to instantiate once per worker. |
| `actor_class_args` | `None` | Positional arguments for that class's constructor. Only valid with `actor_class_name`. |
| `actor_class_kwargs` | `None` | Keyword arguments for that class's constructor. Only valid with `actor_class_name`. |
| `python_paths` | `None` | Extra paths prepended to the workers' `sys.path`. |
| `add_cwd_to_python_path` | `True` | Also add the driver's cwd. |

The executor cloudpickles the actor arguments
and puts them in the `ds-service` map.
The keys are `actor_class_args:<name>` and `actor_class_kwargs:<name>`,
where `<name>` is the job group's name.
Each worker reads them back at startup.

They must be picklable.
Anything they refer to must be importable on the compute node,
exactly as for the actor class itself.
They are not part of the job group's identity,
so a later call can redefine a job group with different ones.
`sbatch_args` are part of it, and a different value asserts.
Only the workers started after that call read the new values.
A worker constructs its actor once, when it starts.

For the task-side view of actors, see
[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md).

## One worker per pilot job, or one per Slurm task

`is_batch_worker` controls how many workers each pilot job starts:

| Setting | Script is run with | Workers per pilot job |
| --- | --- | --- |
| `is_batch_worker=False` (default) | `srun` | one per Slurm task in the allocation |
| `is_batch_worker=True` | sourced directly | one, on the batch host |

With the default, `--nodes=4 --ntasks-per-node=2`
gives 8 workers from a single `scale_jobs(..., 1)` call.
`is_batch_worker=True` gives a single worker
that owns the pilot job's whole allocation,
which is what a multi-node (MPI or UPC++) task needs.

## Errors that end a wait

A task whose queues have no worker can never finish.
`as_completed` and `wait` therefore do not block on such a task.
They raise `RuntimeError` and name those queues,
unless `raise_on_error` is `RAISE_NEVER`.
They check this twice.

**Before the first wait**, and without a call to Slurm,
they require a `scale_jobs` call
for at least one of each pending task's queues.
`submit` does not check queue names,
so this check is where a mistyped queue name appears.
They raise the error before they yield any result.

**Then once a minute while blocked**,
they ask `squeue` whether each pending task's queues
still have a job on the cluster.
That check covers an allocation that ended, jobs that were canceled,
and jobs that died before they drained their queue.
The first of these checks is a minute in, not immediate.
A `squeue` they cannot reach leaves liveness unknown rather than dead.
They log it, retry it, and do not end the wait.

Both checks only know about workers **this executor** started.
They refuse an executor that submits to a queue
where another process launched the pilot jobs.

Two more states end a wait,
and the executor reads both straight off the server:

- **the server does not know the task id**:
  `RuntimeError: Task ... is unknown to the task queue server`.
  In practice this is a `Task` built by hand,
  or one left over from a server that restarted in the meantime.
- **the task was canceled**:
  `RuntimeError: Task ... was canceled on the task queue server`.
  Nothing in this library cancels a task,
  so this means somebody called `task_cancel` through the `ds-service`
  client directly.
  `ds-service` never dispatches a canceled task again.

For what to do about each, see
[How to troubleshoot a failing run](../how-to-guides/troubleshoot-a-failing-run.md).

## `Task`

`submit` returns a `Task` with `task_id`, `queue`, `priority`, `function`,
`input`, and `output`.
`output` is a sentinel until the task completes.
After that it holds the return value,
or a `RemoteExecutionError(error, error_id)` if the worker raised.
That class lives in `slurm_workflows.utils`,
and imports from the package root like everything else.
`wait` and `as_completed` are what fill it in.

`task_name` is a read-only property, and it is `None`
until `executor.set_task_name(task, name)` sets it.
That call stores the name on the server, under `task_name:<task_id>`,
as UTF-8 rather than a pickle.
Anything that reads the map can therefore read it too.
The call also updates the `Task` to match.

Nothing in this library dispatches on the name.
Whoever looks at the queue reads it,
which in practice means [`swtop`](swtop.md).
`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` call it themselves
for every task they submit.

`submit` assigns `priority`, and `priority` orders the queue.
`ds-service` dispatches the highest value first,
and `submit` sets it from a negated wall clock,
so one queue serves its tasks **oldest first**.
The `Task` records it for inspection.
A change there has no effect,
because `submit` sent the value the server orders by
when it enqueued the task.

## `RaiseOnError`

What `as_completed` and `wait` do about a task that fails.
A failure is any of these:

- A task whose worker raised. Its `output` is a `RemoteExecutionError`.
- A task canceled on the server.
- A task the server does not know.
- A pending task whose queues have no pilot job left to run them.

Both calls give up only on the tasks that cannot finish.
Unless the policy raises at once,
they still wait for the rest of the batch.

| Value | Effect |
| --- | --- |
| `RAISE_ON_FIRST_ERROR` | The default. Stop at the first failure and raise `RuntimeError`. |
| `RAISE_AFTER_COMPLETED` | Wait for every task that can still finish, then raise once for all the failures together. `as_completed` treats this as `RAISE_ON_FIRST_ERROR`. |
| `RAISE_NEVER` | Report and return. |

**Both calls warn about every failure on stderr as they meet it**,
whichever value the call was given.
The value decides only whether an exception follows.
The warning carries the task id
and, for a worker that raised,
the `error_id` that appears beside the traceback in that worker's log.

With `RAISE_NEVER` the caller reads the outcome off the tasks:

```python
from slurm_workflows import RaiseOnError, RemoteExecutionError
from slurm_workflows.slurm_pilot_executor import NoOutput

executor.wait(tasks, desc="squaring", raise_on_error=RaiseOnError.RAISE_NEVER)

failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
never_ran = [t for t in tasks if t.output is NoOutput]
```

`output` stays `NoOutput` for a task that was canceled,
is unknown to the server,
or was still pending when the last pilot job went away.

