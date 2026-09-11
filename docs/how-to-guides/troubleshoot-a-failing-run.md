# How to troubleshoot a failing run

[<- back to the main README](../../README.md)

## First, find the right log

Everything for a run lives under the executor's `work_dir`.
The executor prints that path at startup, and `executor.work_dir` holds it.
[Logs](../reference/executor.md#logs) lists which file holds what.

Two shortcuts:

- If a job died immediately, read the generated scripts,
    `<worker-name>.sh` and `<worker-name>.sbatch`.
- If a task raised, grep the work dir for the `error_id`
    inside the `RemoteExecutionError`.
    The same id appears next to the traceback of the failing task.

With the default `is_batch_worker=False`,
a worker's own log goes to the per-task file.
That file is `<worker-name>-<jobid>-<task>.out`, not the batch file.
A failed setup script lands in that same file.
A job of exactly one task is the exception:
it keeps no per-task file, and writes to `<worker-name>-<jobid>.out`.

## Tasks never complete, but jobs are running

The queue name does not match a worker group name,
or the workers cannot reach `ds-service` from the compute nodes.
Check the worker's `-<jobid>-<task>.out` file.

## `RuntimeError: ... tasks are on queues with no worker started`

The executor raises this error as soon as you wait,
because you never called `scale_workers` for those queues.
Either you never scaled the group, or the queue name is a typo.
The executor does not check the queue name at `submit` time,
so compare it against your `define_worker` names.

## `RuntimeError: ... tasks are on queues with no live pilot job`

The executor raises this error while you wait.
You scaled the group, but its jobs then left the cluster.
The cause is the time limit, a cancellation,
or an exit before the queue drained.
The worker's `.out` file says which.
Scale the group back up.
Then submit the tasks again.

## `RuntimeError: Task ... was canceled on the task queue server`

Somebody canceled the task through the `ds-service` client directly.
Nothing in this library cancels a task.

CAUTION: If you still want the output, submit the task again.
The server never dispatches a canceled task a second time,
so the output of that task is lost.

## `RuntimeError: Task ... is unknown to the task queue server`

Two cases produce this error.
The first is a `Task` you built by hand.
The second is a `Task` from a server that restarted since then.

## Jobs start and exit within seconds

The setup script failed.
The worker script runs it,
so the traceback goes to the worker's `.out` file, not to the batch file.

## `ModuleNotFoundError` on a worker

The module is not importable on the compute node.
Add `python_paths=[...]`,
or install it into the environment the setup script activates.

## Nothing is obviously wrong and you want to watch

Run [`swtop`](watch-a-run-with-swtop.md) against the same server address
from another shell.
An empty worker-processes block against a populated worker-jobs block
has two possible causes.
The jobs wait in the queue, or their setup scripts did not finish.

## Related

- [`RaiseOnError`](../reference/executor.md#raiseonerror),
    which collects every failure in a batch, rather than the first one alone
- [Errors that end a wait](../reference/executor.md#errors-that-end-a-wait)
