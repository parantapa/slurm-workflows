# The pilot-job model

[<- back to the main README](../../README.md)

## The problem it solves

One Slurm job per unit of work makes you pay Slurm's queue
once per unit of work.
On a busy cluster that latency dominates everything else
as soon as the individual tasks are small.
An exploration of a few thousand short evaluations
can spend most of its wall clock in Slurm's queue rather than on the work.

The pilot-job model inverts that.
The executor submits a few long-lived pilot jobs once,
and each job starts workers that stay alive.
The executor then dispatches the actual work to those workers over a queue.
You pay Slurm's latency once per worker instead of once per task.
The cluster sees a handful of ordinary jobs.
The program sees something
close to [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).

## Why a queue is a job group's name

[Terminology](../terminology.md) defines the words for these pieces,
and [`define_job_group` options](../reference/executor.md#define_job_group-options) lists what a job group holds.
The queue is the part of that naming which is a design decision
rather than a definition.
A job group's workers claim from the queue that carries the group's own name,
so workers from the job group named `gpu` serve `submit("gpu", ...)`.

The queue-equals-group rule keeps job groups isolated
without any routing configuration.
A `gpu` worker cannot claim a task from the `cpu` queue,
because a worker only ever asks its own queue for work.

## The three processes

| Process | Runs on | Role |
| --- | --- | --- |
| Driver (which uses a `SlurmPilotExecutor`) | login node, or a Slurm job | defines job groups, scales pilot jobs, submits tasks |
| The `ds-service` server | login node (or elsewhere) | holds tasks on named queues |
| Workers | compute nodes | claim tasks, run them, return the task outputs |

`scale_jobs` renders a shell script and an sbatch wrapper
from Jinja templates and submits them.
Each job runs your setup script inline and launches `slurm-pilot-worker`.
That worker loops forever: claim a task from its group's queue,
cloudpickle-load the function, run it, post the cloudpickled task output back.

The driver and the workers never talk to each other.
Everything passes through the server.
For this reason, a worker needs only the server's address,
never the driver's.
The workers also do not need to know how many of them there are.

## Where the driver runs

The driver runs on a login node, or inside a Slurm job.
Both placements are part of the design.
The model is the same in each.
A login node suits a run you start by hand and watch.
A Slurm job suits a run that outlives your terminal.
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

`get_clean_environ` in `slurm_utils.py` exists for that case.
The function drops all four prefixes,
and the executor gives `sbatch` what is left.
A pilot job therefore takes its shape from the job group's
`sbatch_args` alone, whatever the driver runs inside.
The same call runs on a login node, where there is nothing to strip.

## Exceptions are values

An exception raised on a worker
never reaches the driver.
The worker catches it, logs the traceback under a generated `error_id`,
and returns a `RemoteExecutionError` as the task's `output`.
`as_completed` and `wait` are what turn that value back into an exception,
under the [`RaiseOnError`](../reference/executor.md#raiseonerror)
policy you give them.

This trade is deliberate.
A worker that dies on a bad task takes the rest of its queue with it,
so a worker swallows everything.
The cost is that nobody sees a failure
until somebody waits on the task.

## Why one executor per server

A `ds-service` server holds one run's tasks,
worker registrations and actor arguments
in a single flat namespace with no executor name in it.
Point two executors at one server,
and they share that namespace.
Same-named job groups serve each other's tasks,
and they overwrite each other's actor arguments.

Nothing enforces the rule,
because an executor cannot see another one.
That is also why the liveness checks behind
[a wait that cannot finish](../reference/executor.md#errors-that-end-a-wait)
refuse a queue served by pilot jobs the executor did not start.
The executor has no way to tell a healthy foreign worker
from a queue nobody serves.

The executor still prefixes task ids and pilot job names with its own name.
The reason is that a *cluster* holds many runs,
even when a server holds one.

## Which class to reach for

Three classes sit on top of this model,
in increasing order of how much of the loop they own:

| Class | Fits work shaped like |
| --- | --- |
| `SlurmPilotExecutor` | a set of tasks that is known up front |
| `ExploreSpaceSobolQMC` | evaluating one function over one space |
| `OptimizeSpaceBotorch` | finding where one function is smallest |

The two space classes build on the first.
Both take an executor and submit through it,
so every program begins with an executor.
The difference between them is not capability
but who owns the submit-and-wait loop.
Work that is not a function over a space must own that loop itself,
which is what `submit` and `wait` are for.

## Related

- [`SlurmPilotExecutor`](../reference/executor.md)
- [Terminology](../terminology.md),
    for the word this project uses for each thing
- [The trail a run leaves](the-trail-a-run-leaves.md),
    for the trail the three processes leave behind them
- [Computing pi on a Slurm cluster](../tutorials/computing-pi.md),
    which is this model as a program
