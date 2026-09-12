# About the pilot-job model

[<- back to the main README](../../README.md)

## The problem it solves

One Slurm job per unit of work makes you pay the queue
once per unit of work.
On a busy cluster that latency dominates everything else
as soon as the individual tasks are small.
A sweep of a few thousand short evaluations
can spend most of its wall clock in the queue rather than on the work.

The pilot-job model inverts that.
The coordinator submits a few long-lived jobs once,
and each job starts worker processes that stay alive.
The coordinator then dispatches the actual work to those workers over a queue.
You pay Slurm's latency once per worker instead of once per task.
The cluster sees a handful of ordinary jobs.
The program sees something close to
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).

## The vocabulary

**Setup script.** A shell script snippet that every worker runs before it starts.
Use it to set up the environment (`module load`, `conda activate`)
on the compute node.
The executor inlines the text of that script, not a path to it,
into each generated worker script.

**Worker group.** A named recipe for starting a worker:
sbatch arguments, optional setup script, optional actor class.
`define_worker` does not launch workers.
The `scale_workers` method starts and stops them.

**Queue.** You submit a task to a named queue.
A worker group pulls from the queue that matches its own name.
So workers from the group named `gpu` serve `submit("gpu", ...)`.

The queue-equals-group rule keeps groups isolated
without any routing configuration.
A `gpu` worker cannot take a task from the `cpu` queue,
because a worker only ever asks its own queue for work.

## The three processes

| Process | Runs on | Role |
| --- | --- | --- |
| Coordinator (`SlurmPilotExecutor`) | login node, or a Slurm job | defines worker groups, scales pilot jobs, submits tasks |
| `ds-service` | login node (or elsewhere) | holds tasks on named queues |
| Pilot workers | compute nodes | pull tasks, execute them, return results |

`scale_workers` renders a shell script and an sbatch wrapper
from Jinja templates and submits them.
Each job sources your setup script and launches `slurm-pilot-worker`.
That worker loops forever: fetch a task from its group's queue,
cloudpickle-load the function, run it, post the cloudpickled result back.

The coordinator and the workers never talk to each other.
Everything passes through the queue server.
For this reason, you can kill a driver and restart it,
and the workers never notice.
The workers also do not need to know how many of them there are.

## Where the coordinator runs

The coordinator runs on a login node, or inside a Slurm job.
Both placements are part of the design.
The model is the same in each.
A login node suits a run you start by hand and watch.
A Slurm job suits a run that outlives your terminal.
A Slurm job also suits a driver that wants more memory or more cores
than a login node gives it.

A coordinator inside a job submits pilot jobs like any other coordinator.
The difference is the environment it inherits.
Slurm exports `SLURM_*`, `SLURMD_*`, `PMI_*` and `SRUN_*`
into every job it starts.
`sbatch` reads several of those variables as defaults
for the job it submits.
If those variables reach `sbatch`,
a pilot job inherits settings from the coordinator's own allocation.
The coordinator's node count and task count are two of them.

`get_clean_environ` in `slurm_utils.py` exists for that case.
The function drops all four prefixes,
and the executor gives `sbatch` what is left.
A pilot job therefore takes its shape from the worker group's
`sbatch_args` alone, whatever the coordinator runs inside.
The same call runs on a login node, where there is nothing to strip.

## Exceptions are values

An exception raised on a worker
never reaches the coordinator.
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
Point two executors at one server, and they share that namespace.
Same-named worker groups serve each other's tasks,
and same-named groups overwrite each other's actor arguments.

Nothing enforces the rule, because an executor cannot see another one.
That is also why the liveness checks behind
[a wait that cannot finish](../reference/executor.md#errors-that-end-a-wait)
refuse a queue served by pilot jobs the executor did not start.
The executor has no way to tell a healthy foreign worker
from a queue nobody serves.

The executor still prefixes task ids and worker names with its own name,
because a *cluster* holds many runs even when a server holds one.

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
