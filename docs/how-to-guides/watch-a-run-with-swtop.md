# How to watch a run with `swtop`

[<- back to the main README](../../README.md)

`swtop` shows a live view of a running workflow.
The view holds the tasks, the pilot jobs and worker processes,
and the compute nodes they run on.
`swtop` needs nothing on the cluster side.

For the options, the blocks and the columns, see
[`swtop` reference](../reference/swtop.md).

## Watch a run from another shell

From another shell on the login node,
point `swtop` at the same `ds-service` address (`host:port`)
you gave the executor:

```sh
swtop 10.0.0.1:5051
```

If the driver prints its address, copy it from there.
If not, use `ds.address`
from the [queue server](run-the-task-queue-server.md) you started.

You can start `swtop` before the server is up.
`swtop` waits, and fills the blocks once there is something to read.

## Make the tasks block readable

Unless you name a task, `swtop` lists it under `-`:

```python
task = executor.submit("cpu", train, config)
executor.set_task_name(task, "train-7")
```

Name tasks on a run of any size,
since `my-run.task.412` says nothing about which point it is.
You can name a task after you submit it,
and the name appears at the next poll.

`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit.
So a sweep or a search is readable here with no extra work.

## Keep a record of a run instead of a live view

`--plain` prints one frame of text per poll, rather than the live UI.
Redirected to a file, each poll adds one more frame,
rather than replacing the last one:

```sh
swtop 10.0.0.1:5051 --plain > swtop.log
```

The file keeps a record of the run, and you can read it later.
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
