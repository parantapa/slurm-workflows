# The pilot-job model

[<- back to the main README](../../README.md)

## The problem it solves

One Slurm job per unit of work pays the latency of Slurm's queue
once per unit of work.
On a busy cluster that latency dominates everything else
as soon as each unit of work is small.
A map of one function over a few thousand short inputs
can spend most of its wall clock in Slurm's queue rather than on the work.

The pilot-job model inverts that.
The executor submits a few long-lived pilot jobs once,
and each job starts workers that stay alive.
The executor then submits the tasks to a queue,
and those workers claim them from it.
A run pays Slurm's latency once per pilot job instead of once per task.
The cluster sees a handful of ordinary jobs.
The driver sees something
close to [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).

## Why a queue is a job group's name

[Terminology](../terminology.md) defines the words for these pieces,
and [`define_job_group` options](../reference/executor.md#define_job_group-options) lists what a job group holds.
The queue is the part of that naming which is a design decision
rather than a definition.
A job group's workers claim tasks from the queue with the job group's name.
So the workers of the `gpu` job group serve `submit("gpu", ...)`.

This naming rule keeps job groups isolated
without any routing configuration.
A `gpu` worker cannot claim a task from the `cpu` queue,
because the worker loop only ever asks its own queue for work.
The map tasks of `map` and `map_reduce` also claim item tasks
from an item queue of the call's own.
The map tasks run only on the job groups that the call names.

## The three processes

| Process | Runs on | Role |
| --- | --- | --- |
| Driver (which uses a `SlurmPilotExecutor`) | login node, or a Slurm job | defines job groups, scales pilot jobs, submits tasks |
| The `ds-service` server | login node (or elsewhere) | holds tasks on named queues |
| Workers | compute nodes | claim tasks, run them, return the task outputs |

`scale_jobs` renders a shell script and an sbatch wrapper
from Jinja templates and submits them.
Each pilot job runs the job group's setup script inline and launches `slurm-pilot-worker`.
Each worker loops: claim a task from its job group's queue,
load the function with cloudpickle, run it,
and record the cloudpickled task output on the server.
It stops only when its pilot job ends,
or when [`restart_jobs`](../reference/executor.md#restart_jobs) asks it to restart.
A restart starts a new worker in the same pilot job,
so new code reaches the pool without a second wait in Slurm's queue.

The driver and the workers never talk to each other.
Everything passes through the server.
For this reason, a worker needs only the server's address,
never the driver's.
The workers also do not need to know how many of them there are.

## Where the driver runs

The driver runs on a login node, or inside a Slurm job.
Both placements are part of the design.
The model is the same in each.
A login node suits a run that somebody starts by hand and watches.
A Slurm job suits a run that outlives the terminal it started from.
A Slurm job also suits a driver that wants more memory or more cores
than a login node gives it.

A driver inside a job submits pilot jobs like any other driver.
The difference is the environment it inherits.
Slurm exports `SLURM_*`, `SLURMD_*`, `PMI_*` and `SRUN_*`
into every job it starts.
`sbatch` reads several of those variables as defaults
for the job it submits.
If those variables reach `sbatch`,
a pilot job inherits settings from the driver's own allocation.
The driver's node count and Slurm task count are two of them.

The executor handles that case.
It drops every variable with one of the four prefixes from its environment,
and gives `sbatch` what is left.
A pilot job therefore takes its shape
from the job group's `sbatch_args` alone,
whatever the driver runs inside.
The same call runs on a login node, where there is nothing to drop.

## Exceptions are values

An exception raised on a worker
never reaches the driver.
The worker catches it, logs the traceback under a generated `error_id`,
and returns a `RemoteExecutionError` as the task's `output`.
`as_completed` and `wait` are what turn that value back into an exception,
under the [`RaiseOnError`](../reference/submit-and-wait.md#raiseonerror) policy
the driver gives them.

This trade is deliberate.
A worker that dies on a bad task leaves the rest of its queue unserved,
so a worker catches every exception.
The cost is that nobody sees a failure
until somebody waits on the task.

## Why one executor per server

A `ds-service` server holds one run's queues, actor arguments
and restart counters under names that carry the job group's name
and never the executor's.
Two executors that point at one server
share those names.
Same-named job groups serve each other's tasks,
overwrite each other's actor arguments,
and restart each other's workers.

Nothing enforces the rule,
because an executor cannot see another one.
For the same reason, the liveness checks behind
[a wait that cannot finish](../reference/submit-and-wait.md#errors-that-end-a-wait)
refuse a queue whose pilot jobs the executor did not start.
The executor has no way to tell a queue that a healthy foreign worker serves
from a queue that nobody serves.

The executor still prefixes task ids and pilot job names with its own name.
The reason is that a cluster holds many runs,
even when a server holds one run.

## Two interfaces, and who owns the loop

The executor offers two interfaces on top of this model.
They differ in how much of the loop the driver owns.

| Call | Fits work shaped like |
| --- | --- |
| `map` | one function over a collection, with every value back in order |
| `map_reduce` | one function over a collection, folded into one value |
| `submit` with `wait` or `as_completed` | tasks that differ, wait on each other, or need a handle each |

`map` and `map_reduce` are the simple interface.
The call owns the loop.
It enqueues the items, waits for every task, and returns the result.
Neither call divides the items in advance,
so a slow item slows one worker rather than a fixed share of the work.

The cost is control.
The call blocks.
A failure ends the call with nothing back.
The driver sees no handle for the tasks that the call submits.

`submit` with `wait` or `as_completed` is the advanced interface.
The driver owns the loop.
It gets a `Task` per call, and with it task names, priorities,
parent tasks, and the task outputs that finished before a failure.
`map` and `map_reduce` submit their tasks and wait on them
through the same machinery that `submit` and `as_completed` use.

A search over a parameter space owns a loop of its own,
since each round decides what the next one evaluates.
The [`slurm-workflows-optimize`](https://github.com/parantapa/slurm-workflows-optimize)
package builds that loop on `submit` and `wait`.

## Related

- [`SlurmPilotExecutor`](../reference/executor.md)
- [`map`](../reference/map.md)
- [`map_reduce`](../reference/map-reduce.md)
- [`submit`, `wait` and `as_completed`](../reference/submit-and-wait.md)
- [Terminology](../terminology.md),
    for the word this project uses for each thing
- [The monitoring state a run publishes](monitoring-state-a-run-publishes.md),
    for the state the three processes publish as they run
- [Computing pi on a Slurm cluster](../tutorials/computing-pi.md),
    which is this model as a program
