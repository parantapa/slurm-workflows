# `swtop`

[<- back to the main README](../../README.md)

`slurm_workflows.swtop` and `slurm_workflows.swtop_tui`:
the live view of a `slurm-workflows` run.
It shows the tasks, the pilot jobs and the workers,
and the compute nodes they run on.

To watch a run with it, see
[How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md).

## Command line

`swtop` takes the `ds-service` address (`host:port`) the executor was given.

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

`swtop` runs until it is quit or interrupted with Ctrl-C.
Nothing has to be started for it on the cluster side.
The executor and the workers publish what it reads as they go.

## What the screen shows

A summary line of task counts, the progress of the wait the driver is in,
and then five blocks:

```
tasks  waiting 0  ready 118  running 40  finished 240  failed 2  canceled 0  total 400

explore  [###############---------]  242/400 point  60%  working

pilot jobs (1)
NAME              GROUP  JOB      SUBMITTED
my-run.job.cpu.0  cpu    1846231  2026-01-30T10:58:12-05:00

workers (40)
NAME              GROUP  HOST      JOB      PID
my-run.job.cpu.0  cpu    udc-an28  1846231  31402
my-run.job.cpu.0  cpu    udc-an28  1846231  31403

hosts (2)
HOST      FREE MEM  LOAD   /dev/shm  /tmp
udc-an28  212.4G    39.80  0.0%      12.5%
udc-an29  9.1G      40.10  0.0%      98.2%

slurm jobs (1)
JOB      MEMORY  CPU
1846231  148.2G  39.4 cores

tasks (400)
NAME     TASK ID         STATE    WORKER
train-7  my-run.task.7   Running  my-run.job.cpu.0
eval-2   my-run.task.12  Ready
-        my-run.task.13  Ready
```

`swtop` lists tasks running first, then ready and waiting,
then failed, finished, canceled and undefined,
and named before unnamed within each state.

The blocks come from different places:

- **Progress** is what the driver's current `wait` or `as_completed` call
    works through.
    It gives the call's `desc` and `unit`,
    how many of its tasks came back,
    and how far along that is.
    In the terminal UI it is a bar.
    In the text frames it is the line above.
    It is absent until a driver waits on something.
    The last wait's line stays after it finishes,
    marked `done` rather than `working`
    if `swtop` saw it finish.
    A `swtop` started more than a minute after the wait ended
    shows it at 0 and `working`.
    A driver that never waits leaves nothing here.
- **Task counts** are a single RPC, so they always cover every task.
    A server belongs to one executor,
    so every task on it is a task of the run `swtop` watches.
- **Pilot jobs** are the ones the executor submitted.
    The executor publishes each one as it submits it.
    A job appears here the moment `scale_jobs` returns,
    whether or not Slurm started it.
- **Workers** are the ones that registered themselves,
    which each worker does when it starts.
    A job in the pilot jobs block with no worker against it
    is still queued, or its setup script did not finish.
    One job usually holds many workers, one per Slurm task,
    so the two counts differ by design.
- **Hosts and Slurm jobs** are what the monitors sample every 5 seconds:
    see [What the hosts and jobs blocks measure](#what-the-hosts-and-jobs-blocks-measure).
- **Tasks** are all tasks on the server, named or not.

Every block says why it is empty, and never shows a bare header.
A pilot job or a worker whose description the collector
cannot read yet shows `?` in those fields.

## Task names

`swtop` lists a task under `-`
unless `executor.set_task_name(task, name)` named it.
A name published after the task was submitted
appears at the next poll.

`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit.
A point of an exploration gets `<study>-explore-<index>`.
The two kinds of task in a search round get
`<study>-fit-<round>` and `<study>-search-<round>-<index>`.
They zero pad the index to the width of the batch,
so the names sort in submission order.
[`mapreduce`](mapreduce.md) names each map task `<item-queue>.task.<i>`,
and leaves its item tasks unnamed.

## What the hosts and jobs blocks measure

Nothing has to be started for these.
The workers sample the nodes and jobs themselves
and publish the readings:
see [What a run publishes](what-a-run-publishes.md).

The two blocks show these columns:

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
That is what a node without that path looks like.
A path that is not a mount point of its own
shows the filesystem that holds it.

How the workers elect the sampling worker,
and why nothing re-elects it,
is in [The trail a run leaves](../explanation/the-trail-a-run-leaves.md).

## Frames of text instead of a UI

Output that is not a terminal (a pipe, a file, `--plain`)
gets frames of text instead, one per poll.
A header line names the server and the time of the reading:

```
swtop  10.0.0.1:5051  2026-01-30 11:04:57

tasks  waiting 0  ready 118  running 40  finished 240  failed 2  canceled 0  total 400

explore  [###############---------]  242/400 point  60%  working
...
```

On a terminal the frames replace each other.
Redirected output gets them appended instead.
The blocks and columns are the same either way.

## When the server cannot be read

If the server is unreachable, `swtop` says so above the tables.
It continues to poll rather than exit.
In the terminal UI the last good reading stays on the screen,
so a server restart does not blank the display.
A text frame carries the message in place of the blocks.
`swtop` also looks like this when it starts before the server:
it waits, and fills in once there is something to read.
