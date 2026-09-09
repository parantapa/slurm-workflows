# About the pilot-job model

[<- back to the main README](../../README.md)

## The problem it solves

Submitting one Slurm job per unit of work makes you pay the queue
once per unit of work.
On a busy cluster that latency dominates everything else
as soon as the individual tasks are small,
and a sweep of a few thousand short evaluations
can spend most of its wall clock waiting rather than computing.

The pilot-job model inverts that.
A small number of long-lived jobs are submitted once,
each starting worker processes that stay alive,
and the actual work is dispatched to those workers over a queue.
Slurm's latency is paid once per worker instead of once per task.
What the cluster sees is a handful of ordinary jobs;
what the program sees is something close to
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).

## The vocabulary

**Setup script.** A shell script snippet that every worker runs before starting.
This is used to set up the environment (`module load`, `conda activate`)
on the compute node.
The shell script **text** is inlined into each generated worker script.

**Worker group.** A named recipe for starting a worker:
sbatch arguments, optional setup script, optional actor class.
Defining a group does not launch workers.
The `scale_workers` method starts and stops them.

**Queue.** Tasks are submitted to a named queue,
and **a worker group pulls from the queue matching its own name**.
So `submit("gpu", ...)` is served by workers from the group named `gpu`.

The queue-equals-group rule is what keeps groups isolated
without any routing configuration:
a task submitted to `cpu` cannot be picked up by a `gpu` worker,
because a worker only ever asks its own queue for work.

## The three processes

| Process | Runs on | Role |
| --- | --- | --- |
| Coordinator (`SlurmPilotExecutor`) | login node | defines worker groups, scales pilot jobs, submits tasks |
| `ds-service` | login node (or elsewhere) | holds tasks on named queues |
| Pilot workers | compute nodes | pull tasks, execute them, return results |

`scale_workers` renders a shell script and an sbatch wrapper
from Jinja templates and submits them.
Each job sources your setup script and launches `slurm-pilot-worker`,
which loops forever: fetch a task from its group's queue,
cloudpickle-load the function, run it, post the cloudpickled result back.

The coordinator and the workers never talk to each other.
Everything passes through the queue server,
which is why a driver can be killed and restarted
without the workers noticing,
and why the workers do not need to know how many of them there are.

## Two consequences worth knowing

**Exceptions are values.** A task that raises on a worker
does not propagate to the coordinator.
The worker catches it, logs the traceback under a generated `error_id`,
and returns a `RemoteExecutionError` as the task's `output`.
`as_completed` and `wait` are what turn that back into an exception,
under the [`RaiseOnError`](../reference/executor.md#raiseonerror)
policy they are given.

This is a deliberate trade.
A worker that died on a bad task would take the rest of its queue with it,
so a worker swallows everything;
the cost is that a failure is only noticed
when somebody waits on the task.

**Submitting from inside a job works.** `sbatch` is invoked
with all `SLURM_*` / `SLURMD_*` / `PMI_*` / `SRUN_*` variables
stripped from the environment,
so a coordinator running inside a Slurm allocation
can still submit pilot jobs.

## Why one executor per server

A `ds-service` server holds one run's tasks,
worker registrations and actor arguments
in a single flat namespace with no executor name in it.
Point two executors at one server and they share that namespace:
same-named worker groups serve each other's tasks,
and same-named groups overwrite each other's actor arguments.

Nothing enforces the rule, because an executor cannot see another one.
That is also why the liveness checks behind
[a wait that cannot finish](../reference/executor.md#errors-that-end-a-wait)
refuse a queue served by pilot jobs the executor did not start:
it has no way to tell a healthy foreign worker
from a queue nobody is serving.

Task ids and worker names are still prefixed with the executor's name,
because a *cluster* holds many runs even when a server holds one.

## Which class to reach for

Three classes sit on top of this model,
in increasing order of how much of the loop they own:

| Class | Fits work shaped like |
| --- | --- |
| `SlurmPilotExecutor` | a set of tasks that is known up front |
| `ExploreSpaceSobolQMC` | evaluating one function over one space |
| `OptimizeSpaceBotorch` | finding where one function is smallest |

The two space classes are built on the first:
both take an executor and submit through it,
so a program always starts by building one.
The difference between them is not capability
but who owns the submit-and-wait loop.
Work that is not a function over a space has to own that loop itself,
which is what `submit` and `wait` are for.
