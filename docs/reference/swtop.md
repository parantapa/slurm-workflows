# `swtop`

[<- back to the main README](../../README.md)

`slurm_workflows.swtop` and `slurm_workflows.swtop_tui`:
the live view of a `slurm-workflows` run.
It shows the tasks, the pilot jobs and worker processes,
and the compute nodes they run on.

To watch a run with it, see
[How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md).

## Command line

`swtop` takes the `ds-service` address (`host:port`) you gave the executor.

```sh
swtop 10.0.0.1:5051          # every 2 seconds
swtop 10.0.0.1:5051 -i 10    # every 10 seconds
swtop 10.0.0.1:5051 --plain  # frames of text, no UI
```

| Option | Effect |
| --- | --- |
| `-i`, `--interval` | Seconds between polls, `2.0` by default. Must be greater than 0. |
| `--plain` | Print frames of text instead of running the terminal UI. |

| Key | Does |
| --- | --- |
| `q` | Quit |
| `r` | Poll now, rather than waiting for the next interval |

`swtop` runs until you quit it or interrupt it with Ctrl-C.
You start nothing for it on the cluster side.
The executor and the workers publish what it reads as they go.

## What the screen shows

A summary line of task counts, the progress of the wait the driver is in,
and then five blocks:

```
tasks  ready 118  running 40  complete 242  canceled 0  total 400

explore  [###############---------]  242/400 point  60%  working

worker jobs (1)
NAME                  GROUP  JOB      SUBMITTED
my-run.worker.cpu.0   cpu    1846231  2026-01-30T10:58:12-05:00

worker processes (40)
NAME                  GROUP  HOST      JOB      PID
my-run.worker.cpu.0   cpu    udc-an28  1846231  31402
my-run.worker.cpu.1   cpu    udc-an28  1846231  31403

hosts (2)
HOST      FREE MEM  LOAD   /dev/shm  /tmp
udc-an28  212.4G    39.80  0.0%      12.5%
udc-an29  9.1G      40.10  0.0%      98.2%

slurm jobs (1)
JOB      MEMORY  CPU
1846231  148.2G  39.4 cores

tasks (3)
NAME     TASK ID         STATE    WORKER
train-7  my-run.task.7   Running  my-run.worker.cpu.0
eval-2   my-run.task.12  Ready
-        my-run.task.13  Ready
```

`swtop` lists tasks running first, then ready,
then complete, canceled and undefined,
and named before unnamed within each state.

The blocks come from different places:

- **Progress** is what the driver's current `wait` or `as_completed` call
    works through.
    It gives the `desc` and `unit` you gave it,
    how many of its tasks came back,
    and how far along that is.
    In the terminal UI it is a bar.
    In the text frames it is the line above.
    It is absent until a driver waits on something.
    The last wait's line stays after it finishes,
    marked `done` rather than `working`.
    A driver that never waits leaves nothing here.
- **Task counts** are a single RPC, so they always cover every task.
    A server belongs to one executor,
    so every task on it is a task of the run `swtop` watches.
- **Worker jobs** are the pilot jobs the executor submitted.
    The executor publishes each one as it submits it.
    A job appears here the moment `scale_workers` returns,
    whether or not Slurm started it.
- **Worker processes** are the ones that registered themselves,
    which each pilot worker does when it starts.
    A job in the worker jobs block with no process against it
    is still queued, or its setup script did not finish.
    One job usually holds many processes, one per task slot,
    so the two counts differ by design.
- **Hosts and Slurm jobs** are what worker threads sample every 5 seconds:
    see [What the hosts and jobs blocks measure](#what-the-hosts-and-jobs-blocks-measure).
- **Tasks** are all tasks on the server, named or not.

Every block says why it is empty, and never shows a bare header.
A worker job or worker process whose description the collector
cannot read yet shows `?` in the fields it could not read.

## Task names

`swtop` lists a task under `-`
unless you called `executor.set_task_name(task, name)` for it.
A name you publish after you submit the task
appears at the next poll.

`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit.
A point of a sweep gets `<task>-explore-<index>`.
The two kinds of task in a search round get
`<task>-fit-<round>` and `<task>-search-<round>-<index>`.
They zero pad the index to the width of the batch,
so the names sort in submission order.

## What the hosts and jobs blocks measure

You start nothing for these.
The pilot workers sample the nodes and jobs themselves.
One worker per node and one per job runs a sampling thread,
and every 5 seconds each appends to a `ds-service` time series:

| Column | What it is |
| --- | --- |
| `FREE MEM` | Memory available on the node, including the cache the kernel can reclaim |
| `LOAD` | The node's 1 minute load average, over all its cpus |
| `/dev/shm`, `/tmp` | How full each node-local scratch filesystem is |
| `MEMORY` | The job's cgroup total on that node: every process and thread of the job, not just the workers |
| `CPU` | Cores the job used, averaged since the previous sample |

`LOAD` reads against the node's core count.
`bii` has 40 cores,
so 39.80 is a full node and 80 is oversubscribed twice over.
`CPU` reads against what the job asked for,
so `--nodes=1 --ntasks-per-node=40 --cpus-per-task=1` sits near 40.
The first reading of a job is 0,
since the monitor has no earlier sample to difference against.
A `/tmp` that climbs toward 100% takes the whole node down with it,
not only the job that filled it.

A subject marked `(stale)` has no reading in the last minute.
The worker that sampled it is gone:
its job ended, or something killed it.
The remaining workers do not take over the job,
so a run that scales down loses the readings for what it gave up.
A single `-` on an otherwise live row
is one series with nothing recent in it.
That is what a filesystem the node does not mount looks like.

How the workers elect the sampling worker, and why nothing re-elects it, is in
[About what a run publishes](../explanation/about-what-a-run-publishes.md).

## Frames of text instead of a UI

Output that is not a terminal (a pipe, a file, `--plain`)
gets frames of text instead, one per poll.
A header line names the server and the time of the reading:

```
swtop  10.0.0.1:5051  2026-01-30 11:04:57

tasks  ready 118  running 40  complete 242  canceled 0  total 400

explore  [###############---------]  242/400 point  60%  working
...
```

On a terminal the frames replace each other.
When you redirect the output, `swtop` appends them instead.
The blocks and columns are the same either way.

## When the server cannot be read

If the server is unreachable, `swtop` says so above the tables.
It continues to poll rather than exit.
In the terminal UI the last good reading stays on the screen,
so a server restart does not blank the display.
A text frame carries the message in place of the blocks.
`swtop` also looks like this when you start it before the server:
it waits, and fills in once there is something to read.
