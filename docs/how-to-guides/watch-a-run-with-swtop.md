# How to watch a run with `swtop`

[<- back to the main README](../../README.md)

A run in progress tells you almost nothing.
The driver prints little,
and the work is on nodes you are not logged in to.
`swtop` gives you a live view of the tasks, the pilot jobs and the workers,
and of the compute nodes they run on.
It needs nothing on the cluster side.

For the options, the keys, the blocks and the columns, see
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
from the [`ds-service` server](run-the-ds-service-server.md) you started.

You can start `swtop` before the server is up.
`swtop` waits, and fills the tabs once there is something to read.

Each block is a tab.
Press `p`, `w`, `h`, `j` or `t` to show the pilot jobs, workers, hosts,
slurm jobs or tasks tab.

## Make the tasks tab readable

If you are running an exploration or a search, skip this section.
`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit,
and the tasks tab is readable without your help.

Otherwise, name your tasks.
Unless you name a task, `swtop` lists it under `-`:

```python
task = executor.submit("cpu", train, config)
executor.set_task_name(task, "train-7")
```

Name tasks on a run of any size,
since `my-run.task.412` says nothing about which point it is.
You can name a task after you submit it,
and the name appears at the next poll.

The tasks tab shows only waiting, ready and running tasks at start.
To see the tasks that failed, check `Failed` in the row of checkboxes
above the table.
The other states work the same way.

## Keep a record of a run instead of a live view

To keep a record of a run, pass `--plain`
and redirect the output to a file:

```sh
swtop 10.0.0.1:5051 --plain > swtop.log
```

`--plain` prints one frame of text per poll, rather than the live UI.
Redirected to a file, each poll adds one more frame,
rather than replacing the last one.
The file has the same blocks and columns as the tabs,
one block after another,
and you can read it later.

## Poll less often on a long run

On a long run, pass `-i` with the number of seconds between polls:

```sh
swtop 10.0.0.1:5051 -i 10
```

The default is every 2 seconds.

## Related

- [How to troubleshoot a failing run](troubleshoot-a-failing-run.md)
- [How to embed `swtop` in a Textual app](embed-swtop-in-a-textual-app.md)
- [The trail a run leaves](../explanation/the-trail-a-run-leaves.md),
    for why a block can be empty while the run is healthy
