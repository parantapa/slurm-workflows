# About what a run publishes

[<- back to the main README](../../README.md)

A run leaves a trail on the `ds-service` server:
which pilot jobs were submitted, which worker processes started,
what the nodes and jobs are doing, and how far a wait has got.
None of it is needed to run tasks.
It exists so that a run can be watched from outside itself,
which is what [`swtop`](../reference/swtop.md) does.

For the field names and the exact keys, see
[What a run publishes](../reference/executor.md#what-a-run-publishes).

## Two halves, because they are known at different times

The executor writes `worker_job_info:<worker-name>`
as soon as `sbatch` returns.
Each worker process writes `worker_process_info:<worker-id>`
when it starts.

Nothing merges the two, and that is the point.
A pilot job exists from the moment it is submitted,
but the host it will land on and the pids it will run
are not known until Slurm starts it.
Keeping them apart means a queued job is visible before it runs,
and the *difference* between the two blocks
is a fact worth reading: a job listed with no process against it
has not started yet, or its setup script has not finished.

One job usually holds many processes, one per task slot,
so the two counts are expected to differ even when everything is healthy.

## One key per subject, never one per field

Each half is a single JSON object rather than a key per field,
because a reader can poll at any moment.
Five separate writes would let a reader land between two of them
and see a worker whose host it never learned;
worse, a reader that caches what it has seen
would remember that half-described worker for the rest of the run.
One key makes a worker either absent or complete.

A worker writes its identity *before* it builds its actor,
so a worker that dies in its actor's constructor
has still recorded which job and node it died on.

Nothing ever updates or deletes these keys.
That is what makes them cacheable:
a monitor reads each worker's fields once and never again,
which is the difference between one read per poll
and four hundred reads per poll on a large pool.
The store is in memory and dies with the server,
which is the only cleanup there is.

## Why a wait publishes its progress instead of drawing it

`wait` and `as_completed` write the `progress_display` key
and append the completed count to a time series as tasks return.
They print nothing themselves.

A driver that drew its own progress bar would be useless
in the two places these runs usually live:
under `nohup`, where a bar becomes megabytes of control characters
in an output file,
and inside a batch job, where nobody is watching the terminal at all.
Publishing instead means the same run shows a bar
when somebody is watching from another shell
and leaves a clean log when nobody is.

The count is appended at most once a second rather than once per task,
so a batch of thousands does not cost an append apiece.
The key is overwritten by the next call,
so the server holds the display for the most recent wait;
the series holds the history of each.

## Why sampling is elected, and never re-elected

Host and job readings are sampled by the workers themselves,
so nothing extra has to be started on the cluster.
But a node runs one worker per task slot and a job spans many nodes,
so most workers must not sample,
or every reading would be duplicated forty times over.

The election uses a `ds-service` counter.
`counter_get_next_value` hands out distinct, gap-free values,
so the worker told 1 for `host_monitor:<hostname>` takes the node
and the one told 1 for `slurm_job_monitor:<job-id>` takes the job.
No lock, no designated rank,
and no need for the workers to know each other exist.

Nothing hands a subject back when that worker dies.
The series simply stops,
and a reader that sees no point in the last minute
calls the subject `(stale)`.
Re-electing would need a heartbeat and a lease,
which is a substantial amount of machinery for a monitoring convenience,
so the trade made here is that a run which scales down
loses the readings for what it gave up.

Sampling threads are daemons that swallow their own errors,
for the same reason a worker swallows a bad task's exception:
a worker killed at the end of its walltime
must not be held open by a monitor,
and a node briefly unreachable
should leave a gap in the series rather than end it.

## Why a monitor draws a failed poll instead of raising

A monitor that exits when the server blinks
takes the screen down with it,
usually at the least convenient moment.
So an unreachable server is reported above the tables and polling
continues, with the last good reading left on screen.
The same behaviour is what lets `swtop` be started before the server exists:
there is no meaningful difference between a server that has not come up yet
and one that is briefly away.

## The limits of what can be shown

A monitor can only show what an RPC can answer.
The server can count tasks by state and enumerate task ids,
but nothing enumerates workers, hosts or jobs,
so those tables are built by searching the key space
for the keys the workers and monitors publish.
A worker that has not published its identity cannot be listed at all.
That is a property of the server, not a gap to be worked around.

## Related

- [`swtop` reference](../reference/swtop.md)
- [How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md)
