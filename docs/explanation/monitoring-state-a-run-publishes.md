# The monitoring state a run publishes

[<- back to the main README](../../README.md)

A run publishes monitoring state on the `ds-service` server.
That state holds which pilot jobs the executor submitted
and which workers started.
It also holds what the nodes and the jobs do,
and how far a wait got.
No task needs any of it.
It exists so that somebody can watch a run from outside itself,
which is what [`swtop`](../reference/swtop.md) does.

For the field names and the exact keys, see
[What a run publishes](../reference/what-a-run-publishes.md).

## Two halves, because the executor and the worker learn them at different times

The executor writes the pilot job's description
as soon as `sbatch` returns.
Each worker writes its own description
when it starts.

Nothing merges the two descriptions,
and this separation is deliberate.
A pilot job exists from the moment the executor submits it.
But nobody knows the node it will land on or the pids it will run
until Slurm starts it.
Two keys mean that a queued job is visible before it runs.
The difference between the two keys is a fact worth reading.
A pilot job with no worker is in one of three states:
it is still queued,
it still runs its setup script,
or its workers cannot reach the server.

One pilot job usually holds many workers, one per Slurm task,
so the two counts differ even when everything is healthy.

## Start and exit, in keys of their own

A pilot job starts after the executor submits it,
and a pilot job and a worker both exit after they describe themselves.
Each of those times goes in a key of its own:
a start key and an exit key for the pilot job,
and an exit key for the worker.
A worker knows its start time when it writes its description,
so the start time is a field of that key.

With a new key for each event, nothing writes a key twice.
`swtop` still reads each description once.
It learns which pilot jobs and workers exited
from two key searches per poll, one for each kind,
without a read.
If a worker writes its exit into its description instead,
a cached description goes stale.
Then `swtop` must read every description again on every poll.

The batch script publishes the pilot job's two times,
because only the batch script lives exactly as long as the job.
A worker can exit while the pilot job continues,
and a job can end before any of its workers starts.
The batch script has no Python of its own.
It runs the worker script with `--pilot-job-event`,
so the times reach the server through the environment that the setup script builds.

Slurm ends a job with SIGTERM, and with SIGKILL a little later.
The batch script traps SIGTERM, and the worker turns it into `SystemExit`,
so each of them publishes its exit in between.
If SIGKILL or a node failure ends a process,
the process publishes nothing,
and `swtop` still lists it.
Its monitored series stop, and `swtop` marks them stale.

## One key per subject, never one per field

Each half is a single JSON object rather than a key per field,
because a reader can poll at any moment.
Separate writes can let a reader land between two of them
and see a worker whose node it never learned.
Worse, a reader that caches what it read
remembers that half-described worker for the rest of the run.
One key makes a worker either absent or complete.

A worker writes its identity first, and builds its actor after that.
A worker that dies in its actor's constructor
therefore still records which job and node it died on.

Nothing ever updates or deletes these keys,
or the start and exit keys beside them.
Because of this, a reader can cache them.
`swtop` reads each worker's fields once and never again.
On a large pool, this caching is the difference between one read per poll
and four hundred reads per poll.
The server holds these keys in memory, and they die with the server.
The exit of the server is the only cleanup there is.

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

A wait appends its count at most once a second,
and each new wait overwrites the key.
Both rules follow from the same choice.
A display that a reader polls costs a write per second,
not a write per task.
Only the newest wait is worth a key of its own.
[Wait progress](../reference/what-a-run-publishes.md#wait-progress)
has the fields.

## Why the workers elect a monitor once, and never again

The workers sample the node and job readings themselves,
so the cluster runs no extra process.
But a node runs one worker per Slurm task,
and a pilot job spans many nodes.
Most workers must therefore not sample,
or every reading arrives forty times over.

The election uses a `ds-service` counter, the host counter,
keyed on the hostname, the job id and the restart generation.
The counter hands out distinct, gap-free values.
The worker that gets the value 1 samples the node,
the part of the job on that node,
and the GPUs that part can see.
Slurm accounts a job in a separate cgroup on each node,
so no single worker can read the whole of a job that spans nodes.
The election needs no lock and no designated rank,
and the workers do not need to know each other exist.

The host counter carries the job id
because a counter never resets while the server runs.
If the key holds the hostname alone,
only the first job that lands on a node samples that node.
A later job on the same node then has no sampler,
and the node looks stale while that job keeps it busy.
With the job id in the key,
every pilot job samples every node it runs on.
Two jobs that share a node both sample it,
and their readings land in the same series,
since they measure the same node.

The host counter also carries the job group's restart generation,
for the same reason.
[`restart_jobs`](../reference/executor.md#restart_jobs) replaces every worker of a job,
and the sampler exits with the rest.
If the key holds the job id alone,
the counter is already past 1 when the new workers ask.
As a result, the job has no sampler after its first restart.

When the worker that samples a subject dies,
no other worker samples that subject
until a restart of the worker's job group holds a new election.
The series stops,
and a reader that sees no point in the last minute
calls the subject `(stale)`.
A second election needs a heartbeat and a lease.
That machinery costs more than a monitoring convenience is worth.
The trade is that a run which scales down
loses the readings for each node it gave up,
until another pilot job lands on that node.

Monitors are daemon threads that catch and discard their own errors,
for the same reason that a worker catches a bad task's exception.
A monitor must not hold a worker open at the end of its time limit.
A node that is briefly unreachable
must leave a gap in the series rather than end it.

## Why `swtop` draws a failed poll instead of raising

A program that exits when the server is briefly unreachable
clears its screen as it exits,
usually at the least convenient moment.
So `swtop` reports an unreachable server and keeps polling.
The terminal UI reports it below the blocks,
with the last good reading left on screen.
The same behavior lets `swtop` start before the server exists.
To `swtop`, a server that does not exist yet
looks the same as a server that is briefly unreachable.

## The limits of what `swtop` can show

`swtop` can only show what a remote procedure call (RPC) to the server can answer.
The server can count tasks by state and list task ids,
but nothing lists workers, hosts or jobs.
`swtop` therefore builds those blocks from a search of the map and the time series
for the keys the workers and monitors publish.
`swtop` cannot list a worker that never published its identity.
This limit is a property of the server, not a gap to work around.

## Related

- [`swtop` reference](../reference/swtop.md)
- [How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md)
- [What a run publishes](../reference/what-a-run-publishes.md),
    for the keys and fields
- [The pilot-job model](pilot-job-model.md),
    for the processes that publish this state
