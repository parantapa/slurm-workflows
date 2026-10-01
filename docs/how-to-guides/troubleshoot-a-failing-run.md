# How to troubleshoot a failing run

[<- back to the main README](../../README.md)

A run fails in one of three places:

- [In a task on a worker](#a-task-fails-on-its-worker).
- [In a wait on the driver](#a-wait-fails-on-the-driver).
- [In a pilot job that never got as far as a task](#a-pilot-job-never-runs-a-task).

Each one leaves its evidence in a different file,
and none of them puts a traceback in front of you.
This guide says which file to open,
and what to do about the errors you are most likely to see.

## A task fails on its worker

### Start from the `error_id`

A task that raised on its worker
comes back as a [`RemoteExecutionError`](../reference/submit-and-wait.md#task).
The warning the executor prints for it ends with an id:

```text
warning: Task demo.task.17 failed on its worker: division by zero (error_id=ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x)
```

Search the work dir for that id first.
Everything for a run lives under the executor's `work_dir`.
The executor prints the work dir path at startup,
and `executor.work_dir` holds it:

```console
$ grep -rn ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x /path/to/work_dir
/path/to/work_dir/demo.job.cpu.0-4211337-2.out:812:2026-09-11 10:14:03,441:worker_process:ERROR:Error executing demo.task.17: ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x: division by zero
```

One line of output tells you where to look.
The file name identifies the Slurm task whose worker ran the task.
[Logs](../reference/what-a-run-publishes.md#logs) gives what the name holds,
and why one file can hold several workers after a restart.
The line number is where the worker logged the failure.
The traceback starts on the next line.

With the default `is_batch_worker=False`,
the worker's log is the file of its Slurm task, `<job-name>-<jobid>-<rank>.out`,
not the batch file.
If the pilot job holds exactly one Slurm task, the log is `<job-name>-<jobid>.out` instead.
The reason is that such a pilot job keeps no per-task file.
If the setup script failed, see
[Pilot jobs start and exit within seconds](#pilot-jobs-start-and-exit-within-seconds).

Add `-A 40` to print the traceback in the same command:

```console
$ grep -rn -A 40 ERROR_k3n8q1zv7c4b0m2s6h9d5p1f8r3t7w2x /path/to/work_dir
```

The worker generates a fresh id for every failure.
So `grep` finds one line, even when a batch failed a hundred times.
Do not look in `executor.log`,
because only the worker's own log carries the traceback.
For why, see
[Exceptions are values](../explanation/pilot-job-model.md#exceptions-are-values).

The executor warns on stderr for every failure,
whatever `RaiseOnError` value you pass.
For a batch with many failures,
wait with `raise_on_error=RaiseOnError.RAISE_NEVER`.
Then read the ids from the tasks:

```python
failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError) and t.output.error_id]
print([t.output.error_id for t in failed])
```

The task itself holds only `str(e)`, the message of the exception.
The traceback and the rest of the log are in the file `grep` names.
If a task never ran because its parent task failed,
its `error_id` is empty.

If `grep` finds nothing, search the work dir of another run.
Unless you pass `work_dir`, each run gets its own timestamped work dir.
The id belongs to the run that printed it.

### `ModuleNotFoundError` on a worker

The module is not importable on the compute node.
Add `python_paths=[...]`,
or install it into the environment the setup script activates.
This failure comes back like any other task failure,
so the `error_id` leads to the import that failed.

An actor class is the exception.
A worker imports it at startup, before it claims a task,
so that failure has no `error_id`.
The worker exits, and its `.out` file holds the traceback.

## A wait fails on the driver

### `RuntimeError: ... tasks are on, or wait on tasks on, queues with no worker started`

With the default `RaiseOnError`, the executor raises this error as soon as you wait.
The reason is that the executor holds no pilot job for those queues.
The queues can belong to the task itself,
or to a parent task that is not finished yet.
Such a task is starved.

If you never scaled the job group, call `scale_jobs` for it before you wait.

To rule out a typo, compare the queue name against your `define_job_group` names.
For where the executor checks queue names, see
[Errors that end a wait](../reference/submit-and-wait.md#errors-that-end-a-wait).

If you called `scale_jobs(name, 0)` or `stop()`,
the executor holds no pilot job for the job group.
Scale the job group back up before you wait.

`map_reduce` and `map` refuse the same case before they submit anything,
with `RuntimeError: map_reduce targets queues with no worker started`
or `RuntimeError: map targets queues with no worker started`.

### `RuntimeError: ... tasks are on, or wait on tasks on, queues with no live pilot job`

With the default `RaiseOnError`, the executor raises this error while you wait.
The executor checks for live pilot jobs once a minute.
So the error can come up to a minute after the pilot jobs leave.
You scaled the job group, but its pilot jobs then left the cluster.
The pending tasks on its queues are stranded.
The cause is the time limit, a cancellation,
or an exit before the queue drained.
The worker's `.out` file says which.
Scale the job group down to 0.
Then scale it back up.
Then wait on the same tasks again.
The server still holds the tasks that were `Waiting` or `Ready`,
and the new pilot jobs run them.
A task that was `Running` when its pilot job left stays `Running` on the server.
The reason is that no worker can claim that task again.
Submit such a task again.
Then wait on the new `Task` in place of the old one.
A `scale_jobs` call with the old count submits nothing.
The reason is that `scale_jobs` counts every pilot job it submitted,
including the ones that left the cluster.

### `RuntimeError: Task ... was canceled on the task queue server`

Somebody canceled the task, or a task it waits on,
through the `ds-service` client directly.
Nothing in this library cancels a task.

If you still want the output, submit the task again.
The server never dispatches a canceled task a second time.
So the output of the canceled task is lost permanently.

### `RuntimeError: Task ... is unknown to the task queue server`

Two cases produce this error.
The first is a `Task` you built by hand.
The second is a `Task` from a server that restarted since then.

Either way, the server holds no such task,
so there is no task output to read.
Submit the work again through the executor that owns the current run.
A `Task` from an earlier run does not work.

## A pilot job never runs a task

### Pilot jobs start and exit within seconds

The setup script failed.
The worker script runs the setup script,
so the errors of the setup script land in the worker's log.
[Logs](../reference/what-a-run-publishes.md#logs) names that file:
`<job-name>-<jobid>-<rank>.out`,
or `<job-name>-<jobid>.out` for a pilot job of one Slurm task or a batch worker.
The batch script also runs the setup script,
to publish the pilot job's start and exit.
So the same errors land in the batch file `<job-name>-<jobid>.out`.
Open either file.
Fix the script.
Then start the run again.
A second `define_job_group` with a changed `setup_script`
raises `AssertionError`.
So the fix cannot reach a job group that the executor already holds.

If a pilot job exited within seconds,
read the generated scripts in the work dir.
In `<job-name>.sbatch`, check the `#SBATCH` lines
against the `sbatch_args` you passed to `define_job_group`.
In `<job-name>.sh`, check the lines of your setup script.
They run after `. '/etc/profile'`,
and before the line that starts the worker.
Check the `--server-address` and `--python-paths-json` arguments
on that line too.

### Tasks never complete, but the pilot jobs run

Open the worker's `-<jobid>-<rank>.out` file.
If it shows a connection failure,
the workers cannot reach the server from the compute nodes.
Restart the server on an interface the compute nodes can reach,
as [How to run the `ds-service` server](run-the-ds-service-server.md) shows.
A queue that matches no job group name causes a different error.
The wait raises
[the error for queues with no worker started](#runtimeerror--tasks-are-on-or-wait-on-tasks-on-queues-with-no-worker-started)
instead.

### Nothing is obviously wrong and you want to watch

Run [`swtop`](watch-a-run-with-swtop.md) against the same server address
from another shell.
Three causes leave the workers block empty
while the pilot jobs block holds entries.
The pilot jobs are still `PENDING`, their setup scripts did not finish,
or their workers cannot reach the server.
For what `STARTED` shows in each of the first two cases, see
[Where each block comes from](../reference/swtop.md#where-each-block-comes-from).
For workers that cannot reach the server, see
[Tasks never complete, but the pilot jobs run](#tasks-never-complete-but-the-pilot-jobs-run).

If a pilot job left the pilot jobs block within seconds, see
[Pilot jobs start and exit within seconds](#pilot-jobs-start-and-exit-within-seconds).
A pilot job whose workers all exited does not stay behind,
because its batch script publishes the job's own exit.

## Related

- [`RaiseOnError`](../reference/submit-and-wait.md#raiseonerror),
    which collects every failure in a batch, rather than the first one alone
- [Errors that end a wait](../reference/submit-and-wait.md#errors-that-end-a-wait)
