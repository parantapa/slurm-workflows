# Concepts and usage

[<- back to the main README](../README.md)

Everything public is importable from the package root:

```python
from slurm_workflows import SlurmPilotExecutor, RaiseOnError, RemoteExecutionError
```

The botorch names, `OptimizeSpaceBotorch` and `OptimizationTask`,
import from there too,
but are resolved on first use rather than when the package is imported,
so `import slurm_workflows` still works without botorch installed.

This is also the usage guide:
the concepts, a quick start, stateful actors,
how many workers a job starts, running the queue server,
and what to read when a run misbehaves.

## Concepts

**Setup script.** A shell script snippet that every worker runs before starting.
    This is used to set up the environment (`module load`, `conda activate`)
    on the compute node.
    The shell script **text** is inlined into each generated worker script.

**Worker group.** A named recipe for starting a worker:
    sbatch arguments, optional setup script, optional actor class.
    Defining a group does not launch workers.
    The `scale_workers` method starts and stops them.

**Queue.** Tasks are submitted to a named queue,
and **a worker group pulls from the queue matching its own name**.
So `submit("gpu", ...)` is served by workers from the group named `gpu`.

## Quick start

```python
from slurm_workflows import SlurmPilotExecutor

def square(x):
    return x * x

DS_SERVICE_ADDRESS = "HOST-IP:5051"

SETUP_SCRIPT = """
module load gcc/14.2.0
conda activate my-env
"""

# server_address points at your running ds-service instance.
executor = SlurmPilotExecutor(name="my-run", server_address=DS_SERVICE_ADDRESS)

# 1. Describe a kind of worker
executor.define_worker(
    name="cpu",
    sbatch_args=["-A my_alloc", "-p standard", "--cpus-per-task=4", "-t 01:00:00"],
    setup_script=SETUP_SCRIPT,
)

# 2. Launch 4 pilot jobs of that kind.
executor.scale_workers("cpu", 4)

# 3. Submit tasks to a named queue; workers of that group pull from it.
tasks = [executor.submit("cpu", square, i) for i in range(100)]

# 4. Collect results as they complete.
# A task that raised an exception stops this with a RuntimeError,
# after a warning naming the task and its error_id.
for task in executor.as_completed(tasks, desc="squaring"):
    ...  # task.output holds the return value

# 5. Cancel all pilot jobs when done.
executor.close()
```

`sbatch_args` are passed straight through to `sbatch`,
so any Slurm option works.
`submit` returns immediately with a `Task` handle;
`as_completed(tasks)` (or `wait(tasks)`) blocks until results are ready.

You don't have to wait for workers before submitting ---
tasks queue up and are picked up as pilot jobs start running.

## `SlurmPilotExecutor(name, server_address, work_dir=None)`

**One executor per `ds-service` server.**
A server holds one run's tasks, worker registrations and actor arguments,
and everything on it is assumed to belong to the executor that is using it.
Point two executors at one server and they share a queue namespace:
same-named worker groups serve each other's tasks,
and same-named groups overwrite each other's actor arguments.
Give each executor its own server -
[running one from the driver](#running-the-task-queue-server)
costs a `with` block.

`name` identifies the executor.
It prefixes every task id (`<name>.task.<n>`)
and every worker's Slurm job name (`<name>.worker.<group>.<index>`),
and it names the executor's log.
So give two executors on one cluster two names:
a shared one collides on all three,
whether or not they are talking to the same server.
It must start with a letter
and hold only letters, digits, `_` and `-` (`[A-Za-z][A-Za-z0-9_-]*`),
and be at least 3 characters long; anything else raises `ValueError`.

`server_address` is the `host:port` of the `ds-service` server.
`work_dir` defaults to a timestamped directory under
`<platform cache dir>/slurm-workflows/<name>`
(`XDG_CACHE_HOME`-driven on Linux),
so one executor's runs sit together;
generated scripts and all logs land there.

| Method | Purpose |
| --- | --- |
| `define_worker(name, sbatch_args, ...)` | Register a worker group. Idempotent - redefining a group identically is a no-op, redefining it differently asserts. |
| `scale_workers(name, count)` | Submit or cancel pilot jobs so the group has `count` jobs. |
| `submit(queue, fn, *args, **kwargs) -> Task` | Enqueue a task. `queue` is a group name or a list of them; `fn` is a callable, or a method name (`str`) for actor workers. |
| `as_completed(tasks, desc, unit="task", raise_on_error=...)` | Yield tasks as their results arrive. Raises `RuntimeError` rather than blocking forever on a task that can never finish - see below. |
| `wait(tasks, desc, unit="task", raise_on_error=...)` | Same, but discards the iterator - just block until all are done. |
| `set_task_name(task, name)` | Name a task, on the queue server as well as locally. |
| `stop()` | Cancel all pilot jobs, keep the executor usable. |
| `close()` | Cancel all pilot jobs and close the queue-server connection. |

It is also a context manager,
which is the easiest way to be sure pilot jobs are cancelled
even if the block raises:

```python
with SlurmPilotExecutor(name="demo", server_address=address) as executor:
    executor.define_worker(name="cpu", sbatch_args=[...])
    executor.scale_workers("cpu", 4)
    ...
# close() has run: every pilot job is cancelled
# and the queue connection is shut.
```

Leaving the block calls `close()`, so the executor is spent afterwards.
An exception raised inside the block still propagates.

### Watching a wait

`desc` is required, and `unit` names what is being counted,
because neither call prints a progress bar of its own:
they publish what they are working through to the queue server instead,
where [`swtop`](how-to-use-swtop.md) draws it.

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
The count is appended at most once a second while tasks arrive,
so a batch of thousands does not cost an append apiece.

The key is overwritten by the next call,
so the server holds the display for the most recent wait,
and the series holds the history of each.

### Waiting on tasks nothing can run

A task whose queues have no worker can never finish,
so `as_completed` / `wait` raise `RuntimeError` naming those queues
rather than blocking until you give up.
They check this twice, for two different failure modes.

**Before waiting at all**, and without asking Slurm,
they require that `scale_workers` has been called
for at least one of each pending task's queues.
This catches the two mistakes
that would otherwise cost you a minute of staring at a progress bar:
forgetting to scale a group,
and mistyping a queue name (queue names are not validated at `submit` time).
The error is raised before any result is yielded,
so a finished task in the same batch cannot mask a stranded one.

**Then once a minute while blocked**,
they ask `squeue` whether each pending task's queues
still have a job on the cluster -
catching an allocation that ended, jobs that were cancelled,
and jobs that died before draining their queue.
The first of these checks is a minute in, not immediate,
so the documented submit-then-scale order keeps working:
a queue whose job has not started *yet* is not a queue that has lost it.
A `squeue` that cannot be reached leaves liveness unknown rather than dead,
so that case is logged and retried instead of ending the wait.

Both checks only know about workers **this executor** started,
which is the assumption above holding:
an executor that submits to a queue served by pilot jobs
some other process launched will be refused.
Have the executor that waits be the one that scaled the group.

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
  A cancelled task is never dispatched again, so waiting cannot help.

Remaining `define_worker` options:

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

The actor arguments are cloudpickled and put in the `ds-service` key value
store, under `actor_class_args:<name>` and `actor_class_kwargs:<name>`,
where `<name>` is the worker group's name.
Each worker reads them back at startup.
So they must be picklable,
and anything they refer to has to be importable on the compute node,
exactly as for the actor class itself.
They are not part of the group's identity,
so redefining a group with different ones is allowed,
unlike differing `sbatch_args`.
Only the workers started after that call read the new values, though:
an actor is constructed once, when its worker starts.

## Stateful actors

To keep per-worker state warm across tasks,
register an actor class by its importable name.
Each worker instantiates it once at startup,
and you dispatch **method names** (as strings) instead of functions:

```python
# my_pkg/model.py
class Model:
    def __init__(self):
        self.model = load_expensive_model()   # runs once per worker

    def predict(self, x):
        return self.model(x)

    def close(self):                           # optional cleanup hook
        self.model.release()
```

```python
executor.define_worker(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    setup_script=SETUP_SCRIPT,
    actor_class_name="my_pkg.model.Model",
)
executor.scale_workers("gpu", 2)

tasks = [executor.submit("gpu", "predict", item) for item in dataset]
executor.wait(tasks)
```

The class must be importable on the compute node.
By default the executor's current working directory
is added to the workers' `sys.path`; add more with `python_paths=[...]`.

If the class takes constructor arguments,
pass them with `actor_class_args` and `actor_class_kwargs`:

```python
class Model:
    def __init__(self, checkpoint, device="cpu"):
        self.model = load_expensive_model(checkpoint, device)

executor.define_worker(
    name="gpu",
    sbatch_args=["-A my_alloc", "-p gpu", "--gres=gpu:1", "-t 02:00:00"],
    actor_class_name="my_pkg.model.Model",
    actor_class_args=["/project/checkpoints/v3.pt"],
    actor_class_kwargs={"device": "cuda"},
)
```

## One worker per job, or one per task

`is_batch_worker` controls how many worker processes each Slurm job starts:

| Setting | Script is run with | Workers per job |
| --- | --- | --- |
| `is_batch_worker=False` (default) | `srun` | one per Slurm task in the allocation |
| `is_batch_worker=True` | sourced directly | one, on the batch node |

So with the default, `--nodes=4 --ntasks-per-node=2`
gives you 8 worker processes from a single `scale_workers(..., 1)` call.
Use `is_batch_worker=True` when you want a single process
that owns the whole worker allocation.
This is useful for running multi-node (MPI or UPC++) tasks.

## `Task`

`submit` returns a `Task` with `task_id`, `queue`, `priority`, `function`,
`input`, and `output`.
`output` is a sentinel until the task completes;
after that it holds the return value -
or a `RemoteExecutionError(error, error_id)` if the worker raised.

`task_name` is a read-only property, `None` until
`executor.set_task_name(task, name)` is called.
That call is what makes the name real:
it stores the name on the queue server, under `task_name:<task_id>`,
as UTF-8 rather than a pickle, so anything reading the store can read it too,
and updates the `Task` to match.
Nothing in this library dispatches on the name;
it is there for whoever is looking at the queue,
which in practice means [`swtop`](how-to-use-swtop.md).
`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` call it themselves
for every task they submit:
see [Naming tasks](how-to-use-swtop.md#naming-tasks-so-the-tasks-block-is-readable).

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

**Every failure is warned about on stderr as it is met**, whichever value
is used; the value decides only whether an exception follows.
The warning carries the task id and,
for a worker that raised,
the `error_id` that appears beside the traceback in that worker's log.

`as_completed` hands out results one at a time,
so there is no point at which it could still be feeding the caller
*and* have finished -
which is why `RAISE_AFTER_COMPLETED` collapses to `RAISE_ON_FIRST_ERROR` there.
Use it with `wait`, where one failed evaluation of a batch
should not hide the other 39.

With `RAISE_NEVER` the caller reads the outcome off the tasks:

```python
from slurm_workflows import RaiseOnError, RemoteExecutionError
from slurm_workflows.slurm_pilot_executor import NoOutput

executor.wait(tasks, raise_on_error=RaiseOnError.RAISE_NEVER)

failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
never_ran = [t for t in tasks if t.output is NoOutput]
```

`output` stays `NoOutput` for a task that was cancelled,
is unknown to the server,
or was still pending when the last pilot job went away.

## Running the task-queue server

The executor and workers communicate only through a `ds-service` server,
one server per executor.
Starting it from the driver is the simplest way to get that,
and ties the server's life to the run's:

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

with DsServiceServer(interface="ib0", port=5051) as ds:
    ds.wait_until_ready()      # blocks until it accepts connections

    executor = SlurmPilotExecutor(name="my-run", server_address=ds.address)
    ...
```

`DsServiceServer` comes from the `ds-service-client` package,
and the constructor spawns the process,
so the server is already coming up when it returns.
Call `wait_until_ready()` before handing the address to anything.
Omit `port` to get an arbitrary free one.

The server must be reachable from the compute nodes,
so it is bound to the IPv4 address of the `interface` you name -
`ib0` above, the login node's Infiniband interface -
and `ds.address` is the `host:port` the workers then connect to.
Naming an interface that does not exist on that node,
or that has no IPv4 address, raises `ValueError` at construction.

`DsServiceServer` runs `ds-service` from your `PATH`.
`ds_service_bin` (or the `DS_SERVICE_BIN` environment variable)
overrides that.

To watch a run while it is going,
see [How to use `swtop`](how-to-use-swtop.md).

## Worker environment

Inside a task, these environment variables are set:

- `PILOT_WORKER_NAME` - e.g. `demo.worker.cpu.0`
- `PILOT_WORKER_GROUP` - the group name
- `DS_SERVER_ADDRESS` - the queue server address
- plus the usual Slurm variables (`SLURM_JOB_ID`, ...)

A run publishes itself to the `ds-service` key value store in two halves.

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

One key rather than one per field,
so a reader can never catch a worker half-described.
A job listed with no process against it has not started yet,
which is the difference the two blocks of
[`swtop`](how-to-use-swtop.md) show.
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
[`swtop`](how-to-use-swtop.md) displays the result.

| Process | Runs on | Role |
| --- | --- | --- |
| Coordinator (`SlurmPilotExecutor`) | login node | defines worker groups, scales pilot jobs, submits tasks |
| `ds-service` | login node (or elsewhere) | holds tasks on named queues |
| Pilot workers | compute nodes | pull tasks, execute them, return results |

`scale_workers` renders a shell script and an sbatch wrapper
from Jinja templates and submits them.
Each job sources your setup script and launches `slurm-pilot-worker`,
which loops forever: fetch a task from its group's queue,
cloudpickle-load the function, run it, post the cloudpickled result back.

Two details worth knowing:

- **Exceptions are values.** A task that raises on a worker
 does not propagate to the coordinator.
 The worker catches it, logs the traceback under a generated `error_id`,
 and returns a `RemoteExecutionError` as the task's `output`.
 `as_completed` and `wait` are what turn that back into an exception,
 under the [`RaiseOnError`](#raiseonerror) policy they are given.
- **Submitting from inside a job works.** `sbatch` is invoked
    with all `SLURM_*` / `SLURMD_*` / `PMI_*` / `SRUN_*` variables
    stripped from the environment,
    so a coordinator running inside a Slurm allocation
    can still submit pilot jobs.

## Logs and troubleshooting

Everything for a run lives under the executor's `work_dir`
(printed as `executor.work_dir`):

| File | Contents |
| --- | --- |
| `executor.log` | Worker submission and cancellation from the executor's side |
| `<worker-name>.sh`, `<worker-name>.sbatch` | The generated scripts - read these first when a job dies immediately |
| `<worker-name>-<jobid>-<task>.out` | One per worker process: setup-script trace, task-by-task progress, full tracebacks |
| `<worker-name>-<jobid>.out` | The batch job's own output - and the worker's log too, when the job is a single task |

`<worker-name>` is `<executor-name>.worker.<group>.<index>`,
which is also the Slurm job name, so `squeue` shows which run a job belongs to.
The work dir itself defaults to `<cache dir>/slurm-workflows/<executor-name>/<timestamp>`.

Slurm writes those files; the worker process doesn't redirect its own output.
Which of the two you want depends on how the group was defined:

- **`is_batch_worker=False`** (the default) runs the worker under `srun`,
    which fans out over every task in the allocation.
    Each task gets `--output <work-dir>/<worker-name>-%j-%t.out`,
    so `<task>` is the task's rank - that file is the worker's log.
    Without the per-task `--output`
    all of them would interleave into the single batch file.
    `<worker-name>-<jobid>.out` then holds
    only what the batch script itself emitted,
    which in practice means `srun`'s own errors.

    The exception is a job of exactly one task -
    `--ntasks=1`, or `--nodes=1` with nothing else said about tasks.
    One task has nothing to interleave with,
    so it keeps `srun` but drops the `--output`
    and writes to `<worker-name>-<jobid>.out` like a batch worker,
    rather than leaving you two files to open per worker.
    Note this counts tasks in the *job*, not per node:
    `--nodes=4 --ntasks-per-node=1` is four tasks
    and still gets four per-task files.

    Which way a job went is not something you have to reconstruct:
    the batch file opens with the task count the job decided on
    (`Num tasks: 4`), says so when it redirects,
    and traces the `srun` command it ran.
- **`is_batch_worker=True`** runs one worker directly on the batch node,
    with no `srun` and so no per-task file.
    Everything lands in `<worker-name>-<jobid>.out`.

The `error_id` inside a `RemoteExecutionError`
appears verbatim next to the traceback -
grep for it across the work dir to find the failing task's stack.

Common failure modes:

- **Tasks never complete, jobs are running.**
    The queue name doesn't match a worker group name,
    or the workers can't reach `ds-service` from the compute nodes.
    Check the worker's `-<jobid>-<task>.out` file.
- **`RuntimeError: ... tasks are on queues with no worker started`.**
    Raised as soon as you wait,
    because `scale_workers` was never called for those queues.
    Either you forgot to scale the group,
    or the queue name is a typo - it is not checked at `submit` time,
    so compare it against your `define_worker` names.
- **`RuntimeError: Task ... was canceled on the task queue server`.**
    Somebody cancelled the task through the `ds-service` client directly
    -- nothing in this library does.
    A cancelled task is never dispatched again
    and never produces an output,
    so waiting on it is reported rather than retried.
    Resubmit it if you still want it run.
- **`RuntimeError: ... tasks are on queues with no live pilot job`.**
    Raised while waiting: the group *was* scaled,
    but its jobs have since left the cluster -
    time limit reached, cancelled, or exited before draining the queue.
    The worker's `.out` file will say which.
    Scale the group back up and resubmit.
- **Jobs start and exit within seconds.**
    The setup script failed.
    It runs inside the worker script,
    so its trace is in the same `.out` file as the worker's log -
    not the batch one.
- **`ModuleNotFoundError` on a worker.**
    The module isn't importable on the compute node -
    add `python_paths=[...]`
    or install it into the environment the setup script activates.
