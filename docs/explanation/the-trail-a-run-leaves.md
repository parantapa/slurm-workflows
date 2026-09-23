# The trail a run leaves

[<- back to the main README](../../README.md)

A run leaves a trail on the `ds-service` server.
The trail holds which pilot jobs the executor submitted
and which workers started.
It also holds what the nodes and the jobs do,
and how far a wait got.
No task needs any of it.
The trail exists so that somebody can watch a run from outside itself,
which is what [`swtop`](../reference/swtop.md) does.

For the field names and the exact keys, see
[What a run publishes](../reference/what-a-run-publishes.md).

## Two halves, because they are known at different times

The executor writes `pilot_job_info:<job-name>`
as soon as `sbatch` returns.
Each worker writes `worker_info:<worker-id>`
when it starts.

Nothing merges the two,
and that is the point.
A pilot job exists from the moment the executor submits it.
But nobody knows the node it will land on or the pids it will run
until Slurm starts it.
Two keys mean that a queued job is visible before it runs.
The *difference* between the two blocks is a fact worth reading.
A pilot job with no worker against it is still queued,
still inside its setup script,
or runs workers that cannot reach the server.

One pilot job usually holds many workers, one per Slurm task,
so the two counts differ even when everything is healthy.

## One key per subject, never one per field

Each half is a single JSON object rather than a key per field,
because a reader can poll at any moment.
Five separate writes can let a reader land between two of them
and see a worker whose node it never learned.
Worse, a reader that caches what it read
remembers that half-described worker for the rest of the run.
One key makes a worker either absent or complete.

A worker writes its identity *before* it builds its actor.
A worker that dies in its actor's constructor
therefore still records which job and node it died on.

Nothing ever updates or deletes these keys.
That is what makes them cacheable.
`swtop` reads each worker's fields once and never again.
On a large pool that is the difference between one read per poll
and four hundred reads per poll.
The map is in memory and dies with the server,
which is the only cleanup there is.

## Why a wait publishes its progress instead of drawing it

`wait` and `as_completed` write the `progress_display` key
and append the completed count to a time series as tasks return.
They draw no progress bar themselves.

A driver that draws its own progress bar is useless
in the two places these runs usually live.
Under `nohup`, a bar becomes megabytes of control characters
in an output file.
Inside a batch job, nobody watches the terminal at all.
The same run therefore shows a bar to somebody
who watches from another shell,
and leaves a clean log when nobody does.

The cadence and the overwrite rule follow from the same choice.
A display that a reader polls costs a write per second,
not a write per task.
Only the newest wait is worth a key of its own.
[Watching a wait](../reference/what-a-run-publishes.md#watching-a-wait)
has the fields.

## Why sampling is elected, and never re-elected

The workers sample the node and job readings themselves,
so the cluster runs no extra process.
But a node runs one worker per Slurm task,
and a pilot job spans many nodes.
Most workers must therefore not sample,
or every reading arrives forty times over.

The election uses a `ds-service` counter.
`counter_get_next_value` returns distinct, gap-free values.
The worker told 1 for `host_monitor:<hostname>` takes the node,
and the one told 1 for `slurm_job_monitor:<job-id>` takes the job.
No lock, no designated rank,
and no need for the workers to know each other exist.

Nothing hands a subject back when that worker dies.
The series stops,
and a reader that sees no point in the last minute
calls the subject `(stale)`.
A second election needs a heartbeat and a lease,
which is a lot of machinery for a monitoring convenience.
The trade is that a run which scales down
loses the readings for what it gave up.

Sampling threads are daemons that swallow their own errors,
for the same reason a worker swallows a bad task's exception.
A monitor must not hold a worker open at the end of its time limit.
A node that is briefly unreachable
must leave a gap in the series rather than end it.

## Why `swtop` draws a failed poll instead of raising

A program that exits when the server blinks
takes the screen down with it,
usually at the least convenient moment.
So `swtop` reports an unreachable server and keeps polling.
The terminal UI reports it above the blocks,
with the last good reading left on screen.
The same behavior lets you start `swtop` before the server exists.
There is no meaningful difference between a server that is not up yet
and one that is briefly away.

## The limits of what can be shown

`swtop` can only show what an RPC can answer.
The server can count tasks by state and enumerate task ids,
but nothing enumerates workers, hosts or jobs.
`swtop` therefore builds those blocks by searching the map
for the keys the workers and monitors publish.
`swtop` cannot list a worker that never published its identity.
That is a property of the server, not a gap to work around.

## Related

- [`swtop` reference](../reference/swtop.md)
- [How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md)
