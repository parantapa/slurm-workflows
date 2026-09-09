# How to use `swtop`

[<- back to the main README](../README.md)

`swtop` shows a live view of a `slurm-workflows` run:
the tasks, the pilot jobs and worker processes,
and the compute nodes they are running on.

Point it at the same `ds-service` address (`host:port`) the executor was given
and watch a run from another shell on the login node:

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
Nothing has to be started for it on the cluster side:
the executor and the workers publish what it reads as they go.

## What the screen shows

A summary line of task counts, the progress of the wait the driver is in,
and then five blocks:

```
tasks  ready 118  running 40  complete 242  canceled 0  total 400

explore  [##############----------]  242/400 point  61%  working

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

Tasks are listed running first, then ready, then complete and canceled,
and named before unnamed within each state,
so what is happening now is at the top.

The blocks come from different places,
which is worth knowing when one of them looks empty:

- **Progress** is what the driver's current `wait` or `as_completed` call
    is working through: the `desc` and `unit` it was given, how many of its
    tasks have come back, and how far along that is.
    In the terminal UI it is a bar; in the text frames, the line above.
    It is absent until a driver waits on something,
    and the last wait's line stays after it finishes,
    marked `done` rather than `working`.
    A driver that never waits, or one whose tasks are all already back,
    leaves nothing here.
- **Task counts** are a single RPC, so they always cover every task.
    A server belongs to one executor,
    so every task on it is a task of the run you are watching.
- **Worker jobs** are the pilot jobs the executor submitted,
    published as it submitted them.
    A job appears here the moment `scale_workers` returns,
    whether or not Slurm has started it.
- **Worker processes** are the ones that have registered themselves,
    which each pilot worker does when it starts.
    A job in the block above with no process against it
    is one that is still queued, or whose setup script has not finished.
    One job usually holds many processes, one per task slot,
    so the two counts differ by design.
- **Hosts and Slurm jobs** are sampled every 5 seconds by worker threads:
    see [What the hosts and jobs blocks measure](#what-the-hosts-and-jobs-blocks-measure).
- **Tasks** are all tasks on the server, named or not.

Every block says why it is empty rather than showing a bare header,
since an empty block is usually a question.

## Naming tasks so the tasks block is readable

A task is listed under `-` unless it is named:

```python
task = executor.submit("cpu", train, config)
executor.set_task_name(task, "train-7")
```

Names are worth setting on a run of any size,
since `my-run.task.412` says nothing about which point it is.
A name published after the task was submitted
appears at the next poll.

`ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch` name what they submit,
so a sweep or a search is readable here without doing anything:
`<task>-explore-<index>` for a point of a sweep,
`<task>-fit-<round>` and `<task>-search-<round>-<index>`
for the two kinds of task a search round is made of.
The index is zero padded to the width of the batch,
so the names sort in submission order.

## What the hosts and jobs blocks measure

Nothing has to be started for these:
pilot workers do the sampling themselves.

A node runs one worker per task slot,
and a job spans many nodes,
so the workers elect one of themselves per node and one per job
(with a `ds-service` counter, first past the post)
and only those run a sampling thread.
Every 5 seconds each thread appends to a `ds-service` time series:

| Column | What it is |
| --- | --- |
| `FREE MEM` | Memory available on the node, not counting cache |
| `LOAD` | The node's 1 minute load average, over all its cpus |
| `/dev/shm`, `/tmp` | How full each node-local scratch filesystem is |
| `MEMORY` | The job's cgroup total on that node: every process and thread of the job, not just the workers |
| `CPU` | Cores the job used, averaged since the previous sample |

Read `LOAD` against the node's core count
(40 on `bii`, so 39.80 is a full node and 80 is oversubscribed twice over),
and `CPU` against what the job asked for
(`--nodes=1 --ntasks-per-node=40 --cpus-per-task=1` should sit near 40).
A `/tmp` climbing towards 100% is worth catching before it arrives;
it takes the whole node down with it, not just your job.

A subject marked `(stale)` has no reading in the last minute,
which means the worker that was sampling it has gone --
its job ended, or it was killed.
The remaining workers do not take the job over,
so a run that scales down loses the readings for what it gave up.
A single measurement shown as `-` on an otherwise live row
is one series with nothing recent in it,
which is what a filesystem that is not mounted on that node looks like.

## Frames of text instead of a UI

Output that is not a terminal --- a pipe, a file, `--plain` --- gets frames
of text instead, one per poll,
with a header line naming the server and the time of the reading:

```
swtop  10.0.0.1:5051  2026-01-30 11:04:57

tasks  ready 118  running 40  complete 242  canceled 0  total 400

explore  [##############----------]  242/400 point  61%  working
...
```

On a terminal the frames replace each other;
redirected, they are simply appended,
so `swtop addr --plain > swtop.log` keeps a record of a run
that can be read afterwards.
The blocks and columns are the same either way.

## When the server cannot be read

If the server is unreachable,
`swtop` says so above the tables and keeps polling rather than exiting.
The last good reading stays on the screen,
so a server being restarted does not blank the display.
This is also what starting `swtop` before the server looks like:
it waits, and fills in once there is something to read.
