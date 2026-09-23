# How to troubleshoot a failing run

[<- back to the main README](../../README.md)

A run fails in one of three places:

- In a task on a worker.
- In a wait on the driver.
- In a pilot job that never got as far as a task.

Each one leaves its evidence in a different file,
and none of them puts a traceback in front of you.
This guide says which file to open,
and what to do about the errors you are most likely to see.

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
/path/to/work_dir/demo.job.cpu.0-4211337-2.out:812:2026-09-11 10:14:03,441:worker_process:ERROR:Error executing demo.task.17: ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x: division by zero
```

One line of output tells you where to look.
The file name identifies the worker that ran the task.
It carries the pilot job's name, its Slurm job id, and its task rank.
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

The task itself holds only `str(e)`, the message of the exception.
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

## Find the log for the failure

Everything for a run lives under the executor's `work_dir`.
[Logs](../reference/what-a-run-publishes.md#logs) lists which file holds what.

With the default `is_batch_worker=False`,
a worker's own log goes to the file of its Slurm task.
That file is `<job-name>-<jobid>-<rank>.out`, not the batch file.
A failed setup script lands in that same file.
A job of exactly one Slurm task is the exception:
it keeps no such file, and writes to `<job-name>-<jobid>.out`.

If a pilot job died immediately, read the generated scripts,
`<job-name>.sh` and `<job-name>.sbatch`.

## Tasks never complete, but the pilot jobs run

Open the worker's `-<jobid>-<rank>.out` file.
If it shows a connection failure,
the workers cannot reach the server from the compute nodes.
Restart the server on an interface the compute nodes can reach,
following [How to run the `ds-service` server](run-the-ds-service-server.md).
A queue that matches no job group name does not end up here.
The wait raises the error in the next section instead.

## `RuntimeError: ... tasks are on, or wait on tasks on, queues with no worker started`

The executor raises this error as soon as you wait,
because you never called `scale_jobs` for those queues.
The queues can belong to the task itself,
or to a parent task that is not finished yet.
Either you never scaled the job group, or the queue name is a typo.
If you never scaled it, call `scale_jobs` for it before you wait.
The executor does not check the queue name at `submit` time,
so compare it against your `define_job_group` names.

## `RuntimeError: ... tasks are on, or wait on tasks on, queues with no live pilot job`

The executor raises this error while you wait.
You scaled the job group, but its pilot jobs then left the cluster.
The cause is the time limit, a cancellation,
or an exit before the queue drained.
The worker's `.out` file says which.
Scale the job group down to 0, then back up.
`scale_jobs` counts every pilot job it submitted,
the ones that left the cluster included,
so a call with the old count submits nothing.
Then submit the tasks again.

## `RuntimeError: Task ... was canceled on the task queue server`

Somebody canceled the task, or a task it waits on,
through the `ds-service` client directly.
Nothing in this library cancels a task.

If you still want the output, submit the task again.
The server never dispatches a canceled task a second time,
so the output of the canceled one is lost for good.

## `RuntimeError: Task ... is unknown to the task queue server`

Two cases produce this error.
The first is a `Task` you built by hand.
The second is a `Task` from a server that restarted since then.

Either way the server holds no such task,
so there is no task output to read.
Submit the work again through the executor that owns the current run.
A `Task` from an earlier run does not work.

## Pilot jobs start and exit within seconds

The setup script failed.
The worker script runs it,
so the traceback lands in the worker's `<job-name>-<jobid>-<rank>.out` file.
Open that file rather than the batch file,
fix the script,
then start the run again.
A second `define_job_group` with a changed `setup_script`
raises `AssertionError`,
so the fix cannot reach a job group the executor already holds.

## `ModuleNotFoundError` on a worker

The module is not importable on the compute node.
Add `python_paths=[...]`,
or install it into the environment the setup script activates.
This failure comes back like any other task failure,
so the `error_id` leads to the import that failed.

An actor class is the exception.
A worker imports it at startup, before it claims a task,
so that failure has no `error_id`.
The worker exits, and its `.out` file holds the traceback.

## Nothing is obviously wrong and you want to watch

Run [`swtop`](watch-a-run-with-swtop.md) against the same server address
from another shell.
Three causes leave the workers block empty
while the pilot jobs block holds entries.
The pilot jobs are still pending, their setup scripts did not finish,
or their workers cannot reach the server.
For the last two, go to
[Pilot jobs start and exit within seconds](#pilot-jobs-start-and-exit-within-seconds)
and [Tasks never complete, but the pilot jobs run](#tasks-never-complete-but-the-pilot-jobs-run).

## Related

- [`RaiseOnError`](../reference/executor.md#raiseonerror),
    which collects every failure in a batch, rather than the first one alone
- [Errors that end a wait](../reference/executor.md#errors-that-end-a-wait)
