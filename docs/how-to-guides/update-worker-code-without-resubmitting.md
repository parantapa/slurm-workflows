# How to update worker code without resubmitting

[<- back to the main README](../../README.md)

You fixed a bug in a module that your tasks import,
or in your actor class,
and the pilot jobs are already live.
If you cancel them and submit new ones,
you wait in Slurm's queue again.
Restart the workers instead.
Each pilot job keeps its allocation and its place against its time limit,
and its workers start again on the code on disk.

## Check what needs a restart

A worker imports each of your modules once,
and keeps the code it imported until it exits.
A change to any of these items needs a restart:

- a module that a task function comes from
- the actor class, or a module it imports
- the actor arguments you pass to `define_job_group`

A function defined in the driver's own script, in `__main__`,
travels with each task by value.
A change to it reaches the workers with the next task you submit,
so it needs no restart.

A restart does not rerun the setup script,
and it does not change the `sbatch_args`.
For a change to either of those, cancel the pilot jobs.
Then submit new ones.

## Put the new code where the workers import it from

Edit the files in place,
or install the new version into the environment that the setup script activates.
The workers must see the new code at the same path as the old code.

To change the actor arguments as well, call `define_job_group` again.
Give it the arguments of the first call,
with the new actor arguments in place of the old ones.
A definition that differs in anything but the actor arguments
raises `AssertionError`:

```python
executor.define_job_group(
    name="gpu",
    sbatch_args=SBATCH_ARGS,
    setup_script=SETUP_SCRIPT,
    actor_class_name="my_pkg.model.Model",
    actor_class_args=["/project/checkpoints/v4.pt"],
)
```

## Restart the workers of the job group

```python
executor.restart_jobs("gpu")
```

Each worker finishes its current task,
exits, and starts again in the same pilot job.
The call blocks until every old worker exits.
After it returns,
every task that a worker claims runs on the new code.

A long task holds the call as long as it runs.
To bound the wait, pass a timeout in seconds:

```python
try:
    executor.restart_jobs("gpu", timeout=600)
except TimeoutError as e:
    print(e)
```

The `TimeoutError` names the workers that did not restart yet.
They still restart after their current task.

To continue without a wait, pass `wait=False`.
A worker can then claim a task or two on the old code before it restarts.

## Submit the tasks that need the new code

Submit after `restart_jobs` returns:

```python
executor.restart_jobs("gpu")
tasks = [executor.submit("gpu", "predict", item) for item in dataset]
executor.wait(tasks, desc="predict")
```

A task that was already on the queue when you called `restart_jobs`
can still go to an old worker.
This happens in the short time before that worker sees the restart.

## Check that the new workers started

The call warns on stderr about any new worker that exits
before the wait ends.
That warning usually means the new code fails to start,
for example because the actor class no longer imports.
Such a worker does not come back.
Its traceback is in its log in the work dir.
Fix the code.
Then scale the job group down and back up
to replace its pilot jobs.

In [`swtop`](../reference/swtop.md),
each restarted worker leaves the workers block,
and a new one appears with a new pid and a new worker id.

## Related

- [`restart_jobs`](../reference/executor.md#restart_jobs)
- [How to keep per-worker state with actors](keep-per-worker-state-with-actors.md)
- [How to watch a run with `swtop`](watch-a-run-with-swtop.md)
- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md),
    for a `ModuleNotFoundError` from an actor's constructor
