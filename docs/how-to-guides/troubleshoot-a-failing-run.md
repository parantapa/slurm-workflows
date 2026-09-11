# How to troubleshoot a failing run

[<- back to the main README](../../README.md)

## Start from the `error_id`

A task that raised on its worker comes back as
a [`RemoteExecutionError`](../reference/executor.md#task).
The warning the executor prints for it ends with an id:

```
warning: Task demo.task.17 failed on its worker: division by zero (error_id=ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x)
```

**Search the work dir for that id first.**
The executor prints the work dir path at startup,
and `executor.work_dir` holds it:

```console
$ grep -rn ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x /path/to/work_dir
/path/to/work_dir/demo.worker.cpu.0-4211337-2.out:812:2026-09-11 10:14:03,441:worker_process:ERROR:Error executing demo.task.17: ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x: division by zero
```

One line of output tells you where to look.
The file name identifies the worker process that ran the task.
It carries the worker name, its Slurm job id, and its task rank.
The line number is where the worker logged the failure.
The traceback starts on the next line.

Add `-A 40` to print the traceback in the same command:

```console
$ grep -rn -A 40 ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x /path/to/work_dir
```

The worker generates a fresh id for every failure.
So `grep` finds one line, even when a batch failed a hundred times.
Only the worker's own log carries the traceback.
`executor.log` does not, because the executor never saw the exception.

The task itself holds only `str(e)`, the formatted exception.
The traceback and the rest of the log are in the file `grep` named.

The executor warns on stderr for every failure,
whatever `RaiseOnError` value you pass.
For a batch with many failures, read the ids from the tasks instead:

```python
failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
print([t.output.error_id for t in failed])
```

If `grep` finds nothing, search the work dir of another run.
Each run gets its own timestamped work dir,
and the id belongs to the run that printed it.

## Which log holds what

Everything for a run lives under the executor's `work_dir`.
[Logs](../reference/executor.md#logs) lists which file holds what.

With the default `is_batch_worker=False`,
a worker's own log goes to the per-task file.
That file is `<worker-name>-<jobid>-<task>.out`, not the batch file.
A failed setup script lands in that same file.
A job of exactly one task is the exception:
it keeps no per-task file, and writes to `<worker-name>-<jobid>.out`.

If a job died immediately, read the generated scripts,
`<worker-name>.sh` and `<worker-name>.sbatch`.

## Tasks never complete, but the jobs run

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
This failure comes back like any other task failure,
so the `error_id` leads to the import that failed.

## Nothing is obviously wrong and you want to watch

Run [`swtop`](watch-a-run-with-swtop.md) against the same server address
from another shell.
Two causes leave the worker processes block empty
while the worker jobs block holds entries.
The jobs wait in the queue, or their setup scripts did not finish.

## Related

- [`RaiseOnError`](../reference/executor.md#raiseonerror),
    which collects every failure in a batch, rather than the first one alone
- [Errors that end a wait](../reference/executor.md#errors-that-end-a-wait)
