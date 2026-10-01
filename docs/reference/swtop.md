# `swtop`

[<- back to the main README](../../README.md)

`slurm_workflows.swtop` and `slurm_workflows.swtop_tui`:
the live view of a `slurm-workflows` run.
It shows the tasks, the pilot jobs and the workers,
and the compute nodes they run on.
The widgets of the terminal UI
have a page of their own, [`slurm_workflows.swtop_widgets`](swtop-widgets.md).

To watch a run with it, see
[How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md).

## Command line

`swtop` takes the same `ds-service` address (`host:port`) as the executor.

```sh
swtop 10.0.0.1:5051          # every 2 seconds
swtop 10.0.0.1:5051 -i 10    # every 10 seconds
swtop 10.0.0.1:5051 --plain  # frames of text, no UI
```

| Option | Effect |
| --- | --- |
| `-i`, `--interval` | Seconds between polls, `2.0` by default. Must be greater than 0. |
| `--plain` | Frames of text in place of the terminal UI. |

| Key | Effect |
| --- | --- |
| `q` | Quit |
| `r` | Poll now, rather than at the next interval |
| `p` | Show the pilot jobs tab |
| `w` | Show the workers tab |
| `h` | Show the hosts tab |
| `j` | Show the slurm jobs tab |
| `t` | Show the tasks tab |

The terminal UI runs until `q` quits it.
The text frames run until Ctrl-C interrupts them.
`swtop` needs nothing extra on the cluster side.
The executor and the workers publish what `swtop` reads as they go.

## What the screen shows

The terminal UI shows these areas, from top to bottom:

1. A header with the server address and a clock.
2. A summary line of task counts, and the time of the last reading.
3. Five tabs, one for each block.
   Each tab label carries the block's row count, such as `workers (40)`.
   The tasks tab also has a row of checkboxes, one for each task state.
   See [The task state filter](#the-task-state-filter).
4. The progress of the wait the driver is in.
5. The error line, shown only when the last poll failed.
6. A footer that lists the keys.

The text frames show the same blocks:
see [Frames of text instead of a UI](#frames-of-text-instead-of-a-ui).

`swtop` lists the tasks in `Running` first, then `Ready` and `Waiting`,
then `Failed`, `Finished`, `Canceled` and `Undefined`,
and named before unnamed within each state.

### Where each block comes from

The blocks come from different places:

- **Progress** is what the driver's current `wait`
    or `as_completed` call works through.
    It gives the call's `desc` and `unit`,
    how many of its tasks came back,
    and how far along that is.
    In the terminal UI it is a bar.
    In the text frames it is the line below the task counts,
    as in [Frames of text instead of a UI](#frames-of-text-instead-of-a-ui).
    It is absent until a driver waits on something.
    The last wait's line stays after it finishes,
    marked `done` rather than `working`
    if `swtop` saw it finish.
    A `swtop` started more than a minute after the wait ended
    shows it at 0 and `working`.
    A driver that never waits leaves nothing here.
- **Task counts** come from a single RPC, so they always cover every task.
    A server belongs to one executor,
    so every task on it is a task of the run `swtop` watches.
- **Pilot jobs** are the ones the executor submitted
    that did not exit yet.
    The executor publishes each one as it submits it.
    A job appears here the moment `scale_jobs` returns,
    whether or not Slurm started it.
    `STARTED` is `-` while the job is still pending,
    or while its batch script is still in its first run of the setup script.
    The batch script publishes the start through the worker script,
    which runs the setup script first.
    A job leaves the block when it publishes its exit.
- **Workers** are the ones that registered themselves
    and did not exit yet.
    Each worker registers when it starts.
    A started job in the pilot jobs block can show no worker against it.
    Such a job is still inside its setup script,
    its workers start again after a restart,
    or its workers cannot reach the server.
    One job usually holds many workers, one per Slurm task,
    so the two counts differ by design.
- **Hosts and slurm jobs** are what the monitors sample every 5 seconds:
    see [What the hosts and slurm jobs blocks measure](#what-the-hosts-and-slurm-jobs-blocks-measure).
    The Slurm job of a pilot job that exited leaves the block.
    A host stays, marked `(stale)` once its readings stop.
- **Tasks** are all tasks on the server, named or not.

Every block says why it is empty, and never shows a bare header.
A pilot job or a worker
whose description the collector cannot read yet
shows `?` in those fields.

`swtop` learns that a pilot job or a worker exited
from the exit it publishes:
see [What a run publishes](what-a-run-publishes.md).
One that dies of SIGKILL, or with its node,
publishes no exit and stays in its block.

### The task state filter

The tasks tab of the terminal UI shows only the tasks in the checked states.
It has one checkbox for each state,
in the order the tasks tab lists the states.
At start, `Waiting`, `Ready` and `Running` are checked,
so the tab shows only the tasks that did not end.
A click on a checkbox checks or unchecks it.
Tab moves the focus to a checkbox, and Space or Enter checks or unchecks it.
The table changes at once, without a poll,
and the choice stays in effect for the rest of the session.

The tab label counts the tasks the tab shows,
not every task on the server.
The summary line counts every task.
When the server holds tasks but none in a checked state,
the tab says `no tasks in the chosen states`.

### Task names

`swtop` lists a task under `-`
unless `executor.set_task_name(task, name)` named it.
A name published after the executor submitted the task
appears at the next poll.

[`map`](map.md) and [`map_reduce`](map-reduce.md) name each map task `<item-queue>.task.<i>`,
and leave their item tasks unnamed.
`map_reduce` also names its reduce task `<item-queue>.reduce`.
Within a state, the tasks tab lists named tasks before unnamed ones,
and sorts names as text.
A name such as `task-0007` therefore sorts in submission order,
and `task-7` does not.

### What the hosts and slurm jobs blocks measure

The hosts and slurm jobs blocks need nothing extra.
The workers sample the nodes and the Slurm jobs themselves
and publish the readings:
see [What a run publishes](what-a-run-publishes.md).

The two blocks show these columns:

| Column | What it is |
| --- | --- |
| `HOST` | The node the reading comes from |
| `FREE MEM` | Memory available on the node, including the cache the kernel can reclaim |
| `LOAD` | The node's 1-minute load average, over all its cores |
| `/dev/shm`, `/tmp` | How full each node-local scratch filesystem is |
| `JOB` | The Slurm job the reading comes from |
| `MEMORY` | The job's cgroup total on that node: every process and thread of the job, not only the workers. Where the cgroup files cannot be read, it is the summed RSS of the processes in the job's cgroup. Where even that list cannot be read, or holds no process with memory, it covers only the sampling worker and its descendants. |
| `CPU` | Cores the job used on that node, averaged since the previous sample |

`LOAD` reads against the node's core count.
On a 40-core node,
39.80 is a full node and 80 is oversubscribed twice over.

The slurm jobs block has one row for each node of each job.
A job on 4 nodes shows 4 rows,
and its total is the sum of their `MEMORY` or `CPU`.
`CPU` reads against what the job asked for on each node,
so a job with `--ntasks-per-node=40 --cpus-per-task=1` sits near 40 on every row.
The first reading of a job is 0,
since the monitor has no earlier sample to subtract from it.

A `/tmp` that climbs toward 100% makes the whole node fail,
not only the job that filled it.

A subject, that is a node or a job on one node,
is marked `(stale)` when it has no reading in the last minute.
The worker that sampled it is gone:
its job ended, or something killed it.
The remaining workers do not take over that node,
so a run that scales down loses the readings for what it gave up.
A node comes back to life when another pilot job lands on it,
since each pilot job samples every node it runs on.
It also comes back when [`restart_jobs`](executor.md#restart_jobs)
restarts the workers of a job on it,
since the new workers hold a new election.

A single `-` on an otherwise live row
is one series with nothing recent in it.
A node without the path of a `/dev/shm` or `/tmp` column
also shows `-` in that column.
A path that is not a mount point of its own
shows the filesystem that holds it.

How the workers elect the sampling worker,
and why only a restart re-elects it,
is in [The monitoring state a run publishes](../explanation/monitoring-state-a-run-publishes.md).

## Frames of text instead of a UI

Output that is not a terminal (a pipe, a file, `--plain`)
gets frames of text instead, one per poll.
A header line names the server and the time of the reading.
The text frames show the same blocks one after another:

```
swtop  10.0.0.1:5051  2026-01-30 11:04:57

tasks  waiting 0  ready 78  running 80  finished 240  failed 2  canceled 0  total 400

squaring  [###############---------]  242/400 task  60%  working

pilot jobs (2)
NAME              GROUP  JOB      SUBMITTED                  STARTED
my-run.job.cpu.0  cpu    1846231  2026-01-30T10:58:12-05:00  2026-01-30T10:59:40-05:00
my-run.job.cpu.1  cpu    1846232  2026-01-30T10:58:12-05:00  -

workers (80)
NAME              GROUP  HOST      JOB      PID    STARTED
my-run.job.cpu.0  cpu    udc-an28  1846231  31402  2026-01-30T10:59:52-05:00
my-run.job.cpu.0  cpu    udc-an28  1846231  31403  2026-01-30T10:59:52-05:00
...

hosts (2)
HOST      FREE MEM  LOAD   /dev/shm  /tmp
udc-an28  212.4G    39.80  0.0%      12.5%
udc-an29  9.1G      40.10  0.0%      98.2%

slurm jobs (2)
JOB      HOST      MEMORY  CPU
1846231  udc-an28  148.2G  39.4 cores
1846231  udc-an29  364.7G  39.9 cores

tasks (400)
NAME     TASK ID         STATE    WORKER
train-7  my-run.task.7   Running  my-run.job.cpu.0
eval-2   my-run.task.12  Ready
-        my-run.task.13  Ready
...
```

The text frames list the tasks in every state.

On a terminal the frames replace each other.
In redirected output, `swtop` appends each frame instead.
The blocks and columns are the same either way.

## When the server cannot be read

If the server is unreachable, `swtop` says so.
In the terminal UI the error line appears below the tabs.
`swtop` continues to poll rather than exit.
In the terminal UI the last good reading stays on the screen,
so a server restart does not blank the display.
A text frame carries the message in place of the blocks.
`swtop` shows the same error when it starts before the server:
it waits, and fills the screen once there is something to read.

## Related

- [How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md)
- [How to embed `swtop` in a Textual app](../how-to-guides/embed-swtop-in-a-textual-app.md)
- [`slurm_workflows.swtop_widgets`](swtop-widgets.md)
- [What a run publishes](what-a-run-publishes.md)
- [The monitoring state a run publishes](../explanation/monitoring-state-a-run-publishes.md)
