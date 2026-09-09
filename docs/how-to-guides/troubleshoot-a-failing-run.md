# How to troubleshoot a failing run

[<- back to the main README](../../README.md)

## First, find the right log

Everything for a run lives under the executor's `work_dir`,
printed as `executor.work_dir`.
Which file holds what is listed in
[Logs](../reference/executor.md#logs).

Two shortcuts:

- **A job that died immediately** - read the generated scripts,
    `<worker-name>.sh` and `<worker-name>.sbatch`.
- **A task that raised** - the `error_id` inside the `RemoteExecutionError`
    appears verbatim next to the traceback.
    Grep for it across the work dir to find the failing task's stack.

Note which file a worker's own log goes to:
with the default `is_batch_worker=False` it is the per-task file
`<worker-name>-<jobid>-<task>.out`, not the batch file.
That catches people out, because a failed setup script lands there too.

## Tasks never complete, but jobs are running

The queue name does not match a worker group name,
or the workers cannot reach `ds-service` from the compute nodes.
Check the worker's `-<jobid>-<task>.out` file.

## `RuntimeError: ... tasks are on queues with no worker started`

Raised as soon as you wait,
because `scale_workers` was never called for those queues.
Either the group was never scaled,
or the queue name is a typo - it is not checked at `submit` time,
so compare it against your `define_worker` names.

## `RuntimeError: ... tasks are on queues with no live pilot job`

Raised while waiting: the group *was* scaled,
but its jobs have since left the cluster -
time limit reached, cancelled, or exited before draining the queue.
The worker's `.out` file will say which.
Scale the group back up and resubmit.

## `RuntimeError: Task ... was canceled on the task queue server`

Somebody cancelled the task through the `ds-service` client directly;
nothing in this library does.
A cancelled task is never dispatched again
and never produces an output.
Resubmit it if you still want it run.

## `RuntimeError: Task ... is unknown to the task queue server`

In practice a `Task` built by hand,
or one left over from a server that has since been restarted.

## Jobs start and exit within seconds

The setup script failed.
It runs inside the worker script,
so its trace is in the same `.out` file as the worker's log -
not the batch one.

## `ModuleNotFoundError` on a worker

The module is not importable on the compute node.
Add `python_paths=[...]`,
or install it into the environment the setup script activates.

## Nothing is obviously wrong and you want to watch

Run [`swtop`](watch-a-run-with-swtop.md) against the same server address
from another shell.
An empty worker-processes block against a populated worker-jobs block
means the jobs are queued or their setup scripts have not finished.

## Related

- [`RaiseOnError`](../reference/executor.md#raiseonerror),
    for collecting every failure in a batch instead of stopping at the first
- [Errors that end a wait](../reference/executor.md#errors-that-end-a-wait)
