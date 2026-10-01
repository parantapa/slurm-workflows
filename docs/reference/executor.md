# `SlurmPilotExecutor`

[<- back to the main README](../../README.md)

`slurm_workflows.slurm_pilot_executor`:
the executor, how it sets up the pool, and the worker it starts.
The driver runs on a login node, or inside a Slurm job.

The executor has two interfaces for running work.
The simple interface is [`map`](map.md) and [`map_reduce`](map-reduce.md).
Each call runs one function over a whole iterable,
and blocks until it is done.
The advanced interface is [`submit`, `wait` and `as_completed`](submit-and-wait.md).
It gives a `Task` handle per task, task names, priorities, parent tasks,
and the `RaiseOnError` policy that decides what a failure does.
Each has a page of its own,
and so does [what a run publishes](what-a-run-publishes.md).

Every name these pages use is importable from the package root,
except `NoOutput` and `DEFAULT_MAP_TASKS`,
which live in `slurm_workflows.slurm_pilot_executor`:

```python
from slurm_workflows import SlurmPilotExecutor, RaiseOnError, RemoteExecutionError
```

## `SlurmPilotExecutor(name, server_address, work_dir=None)`

```python
from slurm_workflows import SlurmPilotExecutor

executor = SlurmPilotExecutor(name, server_address, work_dir=None)
```

`name` identifies the executor.
It prefixes every task id (`<name>.task.<n>`)
and every pilot job's Slurm job name (`<name>.job.<group>.<index>`).
It also names the executor's logger
and the directory its default work dir sits in.
Two executors on one cluster must have two names.
A shared name collides on all of them,
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

`work_dir` defaults to a timestamped directory
under `<platform cache dir>/slurm-workflows/<name>`
(`XDG_CACHE_HOME`-driven on Linux),
so one executor's runs sit together.
Generated scripts and all logs land there.

## Methods

| Method | What it does |
| --- | --- |
| `define_job_group(name, sbatch_args, ...)` | Registers a job group. Submits nothing. The job group name is also the queue name. A second identical definition registers nothing new, and only rewrites any actor arguments it passes. A definition that differs raises `AssertionError`. |
| `scale_jobs(name, count)` | Submits or cancels pilot jobs so the job group has `count` jobs. The count includes every pilot job the job group submitted and did not cancel, even one that already left the cluster. A second call with the same count therefore submits nothing. `stop()` forgets the jobs, so a later call submits `count` new ones. A job group that `define_job_group` did not register raises `AssertionError`. |
| [`restart_jobs(group, wait=True, timeout=None) -> int`](#restart_jobs) | Restarts the workers of a job group inside its running pilot jobs, so they run the code on disk now. Cancels and submits no Slurm job. Returns the job group's new restart generation. See [`restart_jobs`](#restart_jobs). |
| [`map(queue, map_fn, iterable, ...) -> list`](map.md) | Maps an iterable across the pool and returns one value per item, in the order of the iterable. Blocks. |
| [`map_reduce(queue, map_fn, reduce_fn, iterable, init, ...)`](map-reduce.md) | Maps an iterable across the pool and folds the results into one value, on the workers. Blocks. `init` must be the identity of `reduce_fn`. |
| [`submit(queue, fn, *args, ...) -> Task`](submit-and-wait.md) | Enqueues one task and returns a `Task` straight away. |
| [`set_task_name(task, name)`](submit-and-wait.md#task) | Names a task, on the server as well as locally. |
| [`as_completed(tasks, desc, ...)`](submit-and-wait.md) | Yields tasks as their results arrive. |
| [`wait(tasks, desc, ...)`](submit-and-wait.md) | Blocks until every task is done. |
| `stop()` | Cancels all pilot jobs and keeps the executor usable. |
| `close()` | Cancels all pilot jobs and closes the server connection. |

`SlurmPilotExecutor` is also a context manager.
When the `with` block ends, Python calls `close()`.
That call cancels every pilot job, and the executor is spent afterward.
An exception raised inside the block still propagates.
A whole run is four calls:

```python
with SlurmPilotExecutor("my-run", address) as executor:
    executor.define_job_group(name="cpu", sbatch_args=SBATCH_ARGS, setup_script=SETUP)
    executor.scale_jobs("cpu", 1)

    results = executor.map("cpu", square, range(100))
# close() has run: every pilot job is canceled
# and the server connection is shut.
```

`map` blocks, so a job group named in its queue
must have a pilot job submitted before the call.
`submit` does not block,
and it accepts tasks before any worker exists.
See [`submit`, `wait` and `as_completed`](submit-and-wait.md).

## `define_job_group` options

| Argument | Default | Meaning |
| --- | --- | --- |
| `setup_script` | `""` | Shell snippet run on the compute node before the worker starts. The text, not a path. Must be a `str`. An omitted value (or `""`) leaves each worker with the environment `sbatch` passes on, plus what `/etc/profile` gives it. The worker script always sources `/etc/profile`. |
| `worker_exe` | `"slurm-pilot-worker"` | Worker entry point, for a wrapped or renamed one. |
| `is_batch_worker` | `False` | See [One worker per pilot job, or one per Slurm task](#one-worker-per-pilot-job-or-one-per-slurm-task). |
| `actor_class_name` | `None` | Fully qualified class name to instantiate once per worker. |
| `actor_class_args` | `None` | Positional arguments for that class's constructor. Without `actor_class_name`, raises `ValueError`. |
| `actor_class_kwargs` | `None` | Keyword arguments for that class's constructor. Without `actor_class_name`, raises `ValueError`. |
| `python_paths` | `None` | Extra paths prepended to the workers' `sys.path`, in the order given. |
| `add_cwd_to_python_path` | `True` | Also adds the driver's cwd to the workers' `sys.path`, ahead of every `python_paths` entry. |

The executor writes each element of `sbatch_args` as one `#SBATCH` line
in the generated `<job-name>.sbatch`,
so most Slurm options work.
The executor sets `--job-name` and `--output` itself.

The executor cloudpickles the actor arguments
and puts them in the `ds-service` map.
The keys are `actor_class_args:<group>` and `actor_class_kwargs:<group>`,
where `<group>` is the job group's name.
Each worker reads them back at startup.

The actor arguments must be picklable.
Anything they refer to must be importable on the compute node,
exactly as for the actor class itself.

The actor arguments are not part of the job group's identity,
so a later call can redefine a job group with different ones.
`sbatch_args` are part of it, and a different value raises `AssertionError`.
Only the workers started after that call read the new values.
A worker constructs its actor once, when it starts.
[`restart_jobs`](#restart_jobs) starts new workers in the running pilot jobs,
so they read the new values.

For the task-side view of actors, see
[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md).

### One worker per pilot job, or one per Slurm task

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

## `restart_jobs`

```python
generation = executor.restart_jobs(group, wait=True, timeout=None)
```

`restart_jobs` restarts the workers of the job group `group`,
and keeps its pilot jobs.
Each worker exits, and a new one starts in its place,
inside the same pilot job.
The job keeps its allocation and its place against its time limit.
The call cancels no Slurm job, and submits none.

The new worker is a new Python process.
It imports the code afresh from disk,
and it reads the actor arguments that `define_job_group` last wrote.
The setup script does not run again,
and the sbatch arguments do not change.
A function defined in the driver's `__main__` travels by value
with each task, so it needs no restart.

A worker restarts between tasks, never during one.
A running task finishes on the code it started with.
A pilot job that Slurm did not start yet needs no restart,
since its workers start on the code on disk anyway.

| Argument | Default | Meaning |
| --- | --- | --- |
| `group` | required | A job group that `define_job_group` registered. Any other name raises `AssertionError`. |
| `wait` | `True` | Blocks until every worker that started before the call exits, or its Slurm job ends. |
| `timeout` | `None` | With `wait`, the most seconds to block. `None` waits without limit. A negative value raises `ValueError`. |

**With `wait`**, the call returns once every older worker exits.
An older worker is one that started before the call,
in a pilot job this executor submitted for the job group.
It counts as exited once it published its exit,
or once its Slurm job left `squeue`.
Every task claimed after that point runs on a new worker.
The call can take as long as the longest-running task.
A worker that dies before it publishes its exit
makes the wait last until its Slurm job ends or the timeout expires.

When the timeout expires, the call raises `TimeoutError`
and names up to five of the workers it still waits on.
The restart request stays in place,
so those workers still restart after their current task.

At the end of the wait, the call warns on stderr
about any new worker that already exited.
That exit usually means the new code fails to start.
The worker's log in the work dir holds the traceback.

**Without `wait`**, the call returns at once.
A worker can then claim a task or two on the old code before it restarts.

The call returns the job group's new restart generation.
The executor keeps it on the server,
under the counter `restart_generation:<group>`.
Each call adds one to it.
A worker that sees the counter move past the value it read at startup restarts.
See [What a run publishes](what-a-run-publishes.md#keys-and-time-series).

In [`swtop`](swtop.md), each restarted worker leaves the workers block.
A new worker takes its place,
with a new `PID` and a new `STARTED` time.

For the steps of a restart, see
[How to update worker code without resubmitting](../how-to-guides/update-worker-code-without-resubmitting.md).

## The `slurm-pilot-worker` entry point

`pyproject.toml` installs `slurm-pilot-worker`,
which the generated worker script, `<job-name>.sh`, invokes on the compute node.
It takes six required options.
They are the server address, the pilot job's name, its job group,
its actor class name, the work dir and the worker `sys.path`.
With `--pilot-job-event start` or `--pilot-job-event exit`,
it publishes that the pilot job started or exited, and starts no worker.
The generated batch script runs the worker script with that option
when the job starts and when it exits.

The worker script passes its own arguments on to the entry point.
A driver never calls it.
The `worker_exe` argument
of [`define_job_group`](#define_job_group-options) names it,
or names a wrapper that sets an environment or a profiler around it.
A wrapper named by `worker_exe` must pass the arguments on too.

A worker that [`restart_jobs`](#restart_jobs) asked to exit ends with status 75.
The worker script reads that status as a request to start the worker again.
It then runs the entry point once more,
in the same pilot job and on the same node.
On any other status, the worker script exits with that status.
A wrapper named by `worker_exe` must pass the status on unchanged.

At its time limit or through `scancel`,
Slurm ends a pilot job with SIGTERM,
and with SIGKILL a little later.
The worker turns the SIGTERM into `SystemExit` and calls its own `close()`,
so the actor's `close()` runs too.
The SIGKILL can cut a slow `close()` short.
A SIGKILL on its own, or a node failure, skips it.

### The environment a task sees

Inside a task, these environment variables exist:

- `PILOT_JOB_NAME`, the pilot job's name, for example `demo.job.cpu.0`
- `PILOT_JOB_GROUP`, the job group name
- `PILOT_WORKER_ID`, the id the worker claims tasks under
- `DS_SERVER_ADDRESS`, the server address
- plus the usual Slurm variables, such as `SLURM_JOB_ID`

The worker sets the first four when it starts,
before it builds its actor and before it claims a task.

## Related

- [`map`](map.md)
- [`map_reduce`](map-reduce.md)
- [`submit`, `wait` and `as_completed`](submit-and-wait.md)
- [What a run publishes](what-a-run-publishes.md)
- [`swtop`](swtop.md)
- [How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md)
- [How to update worker code without resubmitting](../how-to-guides/update-worker-code-without-resubmitting.md)
- [How to troubleshoot a failing run](../how-to-guides/troubleshoot-a-failing-run.md)
