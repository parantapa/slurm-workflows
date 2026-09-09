# How to watch a run with `swtop`

[<- back to the main README](../../README.md)

`swtop` shows a live view of a running workflow:
the tasks, the pilot jobs and worker processes,
and the compute nodes they are running on.
Nothing has to be started for it on the cluster side.

For the options, the blocks and the columns, see
[`swtop` reference](../reference/swtop.md).

## Watch a run from another shell

Point it at the same `ds-service` address (`host:port`)
the executor was given,
from another shell on the login node:

```sh
swtop 10.0.0.1:5051
```

If the driver prints its address, copy it from there;
otherwise it is `ds.address` from the
[queue server](run-the-task-queue-server.md) you started.

Starting `swtop` before the server is up is fine:
it waits, and fills in once there is something to read.

## Make the tasks block readable

A task is listed under `-` unless it is named:

```python
task = executor.submit("cpu", train, config)
executor.set_task_name(task, "train-7")
```

Name tasks on a run of any size,
since `my-run.task.412` says nothing about which point it is.
A name published after the task was submitted
appears at the next poll.

`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit,
so a sweep or a search is readable here without doing anything.

## Keep a record of a run instead of a live view

`--plain` prints one frame of text per poll rather than running the UI,
and redirected output is appended rather than replaced:

```sh
swtop 10.0.0.1:5051 --plain > swtop.log
```

That keeps a record of a run that can be read afterwards.
The blocks and columns are the same either way.

## Poll less often on a long run

```sh
swtop 10.0.0.1:5051 -i 10
```

The default is every 2 seconds.

## Related

- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md)
- [About what a run publishes](../explanation/about-what-a-run-publishes.md),
    for why a block can be empty while the run is healthy
