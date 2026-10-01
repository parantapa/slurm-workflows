# `submit`, `wait` and `as_completed`

[<- back to the main README](../../README.md)

`submit`, with `wait` or `as_completed`, is the advanced interface
of [`SlurmPilotExecutor`](executor.md).
`submit` enqueues one task and returns a `Task` handle at once.
`wait` blocks until a set of tasks is done,
and `as_completed` yields each task as it finishes.
By contrast, [`map`](map.md) and [`map_reduce`](map-reduce.md)
run one function over a whole iterable
in a single blocking call.

The interface provides what `map` and `map_reduce` do not:

- A handle per task, to read its output or its failure.
- A name per task, for `swtop` to show.
- A priority that orders the queue.
- Parent tasks that a task waits on.
- A choice of what a failure does, through `RaiseOnError`.
  Two of the choices keep the outputs that came back before the failure.
- Tasks submitted before any worker exists, with no call that blocks.

| Method | What it does |
| --- | --- |
| `submit(queue, fn, *args, task_parents=None, task_priority=0.0, **kwargs) -> Task` | Enqueues one task and returns a `Task` straight away. `queue` is a job group name or a list of them. `fn` is a callable, or a method name (`str`) for a job group with an actor. `task_parents` is a list of the `Task`s this one waits on. `task_priority` orders the queue. See [`submit` options](#submit-options). |
| `set_task_name(task, name)` | Names a task, on the server as well as locally. |
| `as_completed(tasks, desc, unit="task", raise_on_error=...)` | Yields tasks as their results arrive. `desc` and `unit` label the progress `swtop` draws. Reports a task whose queues have no pilot job as a failure, as `raise_on_error` directs, and does not block forever. |
| `wait(tasks, desc, unit="task", raise_on_error=...)` | Blocks until all are done. Unlike `as_completed`, it honors `RAISE_AFTER_COMPLETED`. |

```python
from slurm_workflows import SlurmPilotExecutor

with SlurmPilotExecutor("my-run", address) as executor:
    executor.define_job_group(name="cpu", sbatch_args=SBATCH_ARGS, setup_script=SETUP)
    executor.scale_jobs("cpu", 1)

    tasks = [executor.submit("cpu", square, i) for i in range(100)]
    executor.wait(tasks, desc="squaring")

results = [task.output for task in tasks]
```

The executor accepts tasks before any worker exists:
they wait on the queue until a worker claims them.

## `submit` options

| Argument | Default | Meaning |
| --- | --- | --- |
| `task_parents` | `None` | The `Task`s this one waits on. |
| `task_priority` | `0.0` | Orders the queue. The highest value runs first. |

`task_priority` sets the task's `priority`, and `priority` orders the queue.
`ds-service` dispatches the highest value first.
`ds-service` serves tasks of equal priority on one queue oldest first.
The default is `0.0`,
so tasks submitted without a priority run in submission order.

`task_parents` makes a task wait for other tasks.
The server dispatches the task only after every parent finishes.
If a parent fails, the task fails too.
If a parent is canceled, the task is canceled.

Each parent must already be on the server.
A parent therefore goes through `submit` before the task that waits on it.
If the server does not know a parent, `submit` raises `KeyError`.

`fn` cannot take keyword arguments named `task_parents` or `task_priority`,
because `submit` keeps them.

## `Task`

`submit` returns a `Task` with `task_id`, `queue`, `priority`, `function`,
`input`, `output`, and `parent_task_ids`.
`output` is the sentinel `NoOutput` until the task completes.
After that it holds the return value,
or a `RemoteExecutionError(error, error_id)` if the worker raised.
`wait` and `as_completed` set it.
If a parent task failed, the task does not run,
and `output` is a `RemoteExecutionError` with an empty `error_id`.
Its `error` is `Dependency failed (task_id=<id>)`,
and `<id>` is the task that failed.

`RemoteExecutionError` lives in `slurm_workflows.utils`,
and is also importable from the package root.

`priority` and `parent_task_ids` record what `submit` was given
as `task_priority` and `task_parents`:
see [`submit` options](#submit-options).
A change to `priority` on the `Task` has no effect,
because the server orders by the value that `submit` sent with the task.

`task_name` is a read-only property, and it is `None`
until `executor.set_task_name(task, name)` sets it.
That call stores the name on the server, under `task_name:<task_id>`,
as UTF-8 rather than a pickle.
Anything that reads the map can therefore read it too.
The call also updates the `Task` to match.

Nothing in this library dispatches on the name.
Whoever looks at the queue reads it,
which in practice means [`swtop`](swtop.md).
[`map`](map.md) and [`map_reduce`](map-reduce.md) call `set_task_name` for every map task they submit.

## Errors that end a wait

A task whose queues have no pilot job can never finish.
`as_completed` and `wait` therefore do not block on such a task.
They raise `RuntimeError` and name those queues,
unless `raise_on_error` is `RAISE_NEVER`.
They check this twice.

**Before the first wait**, and without a call to Slurm,
they check each pending task's queues.
At least one of those queues must have a pilot job
that `scale_jobs` submitted
and that `stop()` did not cancel later.
`submit` does not check queue names,
so this check is where a mistyped queue name appears.
Under `RAISE_ON_FIRST_ERROR`, they raise the error before they yield any result.

**Then once a minute while they block**,
they ask `squeue` whether each pending task's queues
still have a job on the cluster.
That check covers an allocation that ended, jobs that were canceled,
and jobs that died before they drained their queue.
The first of these checks is a minute in, not immediate.
A `squeue` they cannot reach leaves liveness unknown rather than dead.
They log it, retry it, and do not end the wait.

For a task that waits on a parent,
both calls also check the queues of every unfinished ancestor.
An ancestor is a parent, a grandparent, or any task further up.
For a task whose ancestor can never run,
the calls therefore raise the same error,
whether or not the wait includes that ancestor.

Both checks know only about the pilot jobs that this executor submitted.
They fail a wait on a queue
where another process submitted the pilot jobs.

Two more states end a wait,
and the executor reads both straight off the server:

- **The server does not know the task id**:
  `RuntimeError: Task ... is unknown to the task queue server`.
  In practice, the unknown task is a `Task` built by hand,
  or one left over from a server that restarted in the meantime.
- **The task was canceled**:
  `RuntimeError: Task ... was canceled on the task queue server`.
  Nothing in this library cancels a task.
  Somebody called `task_cancel` through the `ds-service` client directly,
  on this task or on one of its ancestors.
  `ds-service` never dispatches a canceled task again.

For what to do about each, see
[How to troubleshoot a failing run](../how-to-guides/troubleshoot-a-failing-run.md).

## `RaiseOnError`

`RaiseOnError` decides what `as_completed` and `wait` do about a task that fails.
A failure is any of these:

- A task whose worker raised.
  Its `output` is a `RemoteExecutionError`.
- A task whose parent task failed.
  Its `output` is a `RemoteExecutionError`.
- A task canceled on the server, or one whose parent task was canceled.
- A task the server does not know.
- A pending task whose queues have no pilot job left to run them.

Both calls give up only on the tasks that cannot finish.
Unless the policy raises at once,
they still wait for the rest of the tasks.

| Value | Effect |
| --- | --- |
| `RAISE_ON_FIRST_ERROR` | The default. Stops at the first failure and raises `RuntimeError`. |
| `RAISE_AFTER_COMPLETED` | Waits for every task that can still finish, then raises once for all the failures together. `as_completed` treats this as `RAISE_ON_FIRST_ERROR`. |
| `RAISE_NEVER` | Reports and returns. |

**Both calls warn about every failure on stderr as they meet it**,
whatever value `raise_on_error` holds.
The value decides only whether an exception follows.
The warning carries the task id
and, for a worker that raised,
the `error_id` that appears beside the traceback in that worker's log.
The warning for tasks whose queues have no pilot job
counts those tasks and names their queues instead.

With `RAISE_NEVER`, the driver reads the outcome off the tasks:

```python
from slurm_workflows import RaiseOnError, RemoteExecutionError
from slurm_workflows.slurm_pilot_executor import NoOutput

executor.wait(tasks, desc="squaring", raise_on_error=RaiseOnError.RAISE_NEVER)

failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
never_ran = [t for t in tasks if t.output is NoOutput]
```

`output` stays `NoOutput` for a task that was canceled
or is unknown to the server.
It also stays `NoOutput` for a task still pending
on queues with no pilot job to run it.

With `RAISE_AFTER_COMPLETED` or `RAISE_NEVER`,
the outputs of the tasks that finished are still there to read.
However, `map` and `map_reduce` raise `RuntimeError` when a task fails,
and give back no values.

`as_completed` takes the same arguments as `wait`,
and yields each task as it finishes:

```python
total = 0.0
for task in executor.as_completed(tasks, desc="squaring"):
    total += task.output
```

## Related

- [`SlurmPilotExecutor`](executor.md)
- [`map`](map.md)
- [`map_reduce`](map-reduce.md)
- [Computing pi with `submit` and `wait`](../tutorials/computing-pi-with-submit.md)
- [How to troubleshoot a failing run](../how-to-guides/troubleshoot-a-failing-run.md)
