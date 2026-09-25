# Terminology

[<- back to the main README](../README.md)

The words this project uses, one per concept.

This file binds prose and identifiers alike.
It covers the docstrings, the comments, the error messages and the CLI help.
It covers every document under `docs/` and the README,
and the names of classes, methods, arguments and published keys.

A reader of this library holds three vocabularies at once,
because a run spans three systems.
Where they disagree with each other, this file says which sense wins
and how to spell the other one.

## Where the words come from

Three sources, in this order of precedence:

1. **Slurm**, for anything that exists on the cluster.
    Slurm named it. This library does not get a vote.
2. **`ds-service`**, for anything that exists on the server.
    The client API is the spelling, down to the case.
3. **This library's own public API**,
    where neither of the first two has a word for the thing.

Do not invent a word where a source already decided.
Do not spend a source's word on something that source does not mean by it.

Two facts from `ds_service_client` settle the hard cases,
so they are worth stating before the tables:

* `task_get(worker_id, queue)` is documented
  as "Claim a task for `worker_id` from the first queue holding one".
  In `ds-service` a **worker** is whatever claims a task.
  Here the claimant is the process,
  because `PilotWorker` passes its own `worker_id`.
  A worker is therefore a process, and nothing else is a worker.
* `task_add` is documented
  as "Register a task, and enqueue it on each of its queues once it is Ready".
  A **task** is the `ds-service` unit of work.
  Slurm also calls a process inside a job step a task.
  That second sense always carries the word Slurm in front of it.

## The four rules

1. **Bare "task" means a `ds-service` task.**
    Every other sense is qualified, every time.
2. **The driver is a process. The executor is an object.**
    If a sentence stays true with two executors in one program,
    it is about the driver.
3. **A worker is a process.
    A pilot job is a Slurm job.
    A job group is a group of pilot jobs.**
4. **One name per thing.**
    Where a paragraph wants a second word for variety, it does without.

## Processes and places

| Term | What it names | Source | Do not use |
| --- | --- | --- | --- |
| **driver** | The process that defines job groups, submits tasks and waits. The program somebody writes and runs. | this library | coordinator, the caller, the client program, the executor |
| **executor** | The `SlurmPilotExecutor` instance the driver uses to do that. An object, never a process. | `SlurmPilotExecutor` | driver, coordinator, the client |
| **worker** | One process that claims and runs tasks. | `ds-service` (`worker_id`) | worker process, pilot worker, slot |
| **worker id** | `<job-name>.<job-id>.<hostname>.<pid>`, the string a worker claims tasks under. | `ds-service` (`worker_id`) | worker key, process id |
| **pilot job** | The Slurm job that hosts workers. | Slurm, plus this library's qualifier | worker job, batch job, bare "job" beside Slurm jobs |
| **job id**, **job name** | The Slurm job's id and name. `%j` is the id in an output pattern. | Slurm | jobid, slurm_job in prose |
| **job group** | The named recipe for a pilot job, and the queue the workers in those jobs serve. | this library | worker group, worker type, kind of worker, pool |
| **node** | A compute node. Qualify as **login node** or **compute node** where it matters. | Slurm | host, box, machine |
| **batch host** | The node the batch script runs on, where `is_batch_worker=True` puts its one worker. | Slurm | batch node, head node |
| **partition** | Slurm's `--partition`. | Slurm | queue, pool |
| **allocation** | What a running job holds. | Slurm | reservation, the job's resources |
| **actor** | The object a worker builds once at startup. One worker, one actor. | this library | handler, service, model |
| **pool** | Every worker of one job group, or of the run. Say which. | this library | pool of nodes, slots |

### Driver against executor

The split is process against object.
It carries weight because roughly half the sentences
in this documentation are about one, and half about the other.

The **driver** runs somewhere.
It is what a login node hosts,
what `nohup` detaches,
what a batch job can hold instead,
and what the workers never talk to.
A fold that does not happen on a worker happens on the driver.

The **executor** submits, waits, scales and cancels.
It has a `name`, a `work_dir` and a log of its own.
It is what a `with` block closes,
and it is what the one-executor-per-server rule is about.

So: "the driver runs on a login node",
"the executor cancels every pilot job when the block ends",
"botorch must be importable on the driver",
"two executors on one server share a queue namespace".

### Job group, not worker group

A group is a recipe for **pilot jobs**.
Scaling a group sets how many jobs it has,
and the group holds `SlurmJob` objects.
How many workers those jobs start
is decided by the Slurm task count in its sbatch arguments and by `is_batch_worker`,
which the group does not control.
One group scaled to a single job can hold eighty workers.

Write **job group** in full, never bare "group".
Slurm's own "group" is the Unix group in `--gid`.

### A queue is never a partition

The two pi examples name a job group `bii`,
after the partition its jobs run on.
That is legal.
But it reads as though the two were the same thing.
Name a group after what its workers do (`eval`, `optimizer`, `cpu`),
and leave `bii` to `--partition`.

## Work

| Term | What it names | Source | Do not use |
| --- | --- | --- | --- |
| **task** | The `ds-service` unit of work. Unqualified, this is the only thing it means. | `ds-service` | job, work item, future |
| **task id** | `<executor-name>.task.<n>`. | `ds-service` (`task_id`) | task key, task name |
| **task name** | The label `set_task_name` writes to `task_name:<task-id>`. Nothing dispatches on it. | this library | task label, description |
| **parent task** | A task another task waits on, given to `submit` as `task_parents`. The server dispatches a task only after every parent finishes. | `ds-service` (`parent_task_ids`) | predecessor, upstream task, prerequisite |
| **Slurm task** | A process in a job step, what `--ntasks` counts. Always qualified. | Slurm | bare "task", rank, process |
| **task rank** | The index of a Slurm task within its job, `%t` in an output pattern. | Slurm | bare "task", task number, task slot |
| **queue** | A named `ds-service` queue. Equal to a job group name, except for the mapreduce item queue. | `ds-service` | channel, topic, the server, partition |
| **submit** | What the executor does with a task, and what `sbatch` does with a pilot job. Always name the object. | both | push, post, launch a task |
| **enqueue** | What `task_add` does to a task on each of its queues. | `ds-service` | add, register, queue up |
| **claim** | What a worker does to a task through `task_get`. | `ds-service` | pull, take, fetch, grab |
| **task output** | What `task_done` recorded, which `Task.output` holds. | `ds-service` (`output`) | result, return value, answer |
| **setup script** | The shell text inlined into the generated worker script. | this library | env script, prologue, bootstrap |
| **work dir** | The executor's directory of scripts and logs. | `work_dir` | run directory, output directory |

### Mapreduce

One `mapreduce` call has two kinds of task and one kind of item.
Name all three.

| Term | What it names | Do not use |
| --- | --- | --- |
| **item** | One element of `iterable`. | task, unit, record |
| **item task** | The task that carries one item, `<queue>.item.<i>`. It holds no function. | item alone, mapreduce task |
| **item queue** | `<executor-name>.mapreduce.<n>.<token>`. No job group serves it. | mapreduce queue, the private queue |
| **map task** | The task that claims item tasks and folds them, with the task name `<item-queue>.task.<i>`. | mapreduce task, folding task, worker task |
| **partial result** | What one map task returns. | partial, shard, chunk result |
| **chunk** | Several items batched into one item, to amortize the round trip. | group, batch, block |
| **fold** | Applying `reduce_fn`. One verb for the worker-side and the driver-side fold alike. | reduce, accumulate, combine |

## States

Take these verbatim from whichever source owns them.
The two sources disagree on the spelling of one word,
and that is not a typo to correct.

| States | Owner | Note |
| --- | --- | --- |
| `Waiting`, `Ready`, `Running`, `Finished`, `Failed`, `Canceled`, `Undefined` | `ds-service` `TaskState` | One `l` in `Canceled`. |
| `PENDING`, `RUNNING`, `COMPLETED`, `CANCELLED`, `TIMEOUT` | Slurm job states | Two `l`s in `CANCELLED`. |

For the waiting side's own view of a task:

| Term | What it names | Do not use |
| --- | --- | --- |
| **pending** | A task that is `Waiting`, `Ready` or `Running`, seen from a wait. | in flight, outstanding, unfinished |
| **failure** | Any of the five things `RaiseOnError` treats as one. | error, problem, bad task |
| **starved** | A pending task whose queues, or the queues of an unfinished ancestor, have no pilot job in the executor's job groups. | orphaned, unserved |
| **stranded** | A pending task whose queues, or the queues of an unfinished ancestor, have no live pilot job left. | dead, abandoned, lost |

**starved** and **stranded** are the words `_starved_tasks`
and `_stranded_tasks` already use.
They are precise, and they belong in the error text
and in the troubleshooting guide as well as in the code.

## The server

One process, one name.

| Term | What it names | Do not use |
| --- | --- | --- |
| **the `ds-service` server**, short form **the server** | The one process everything talks through. | queue server, task queue server, task-queue server, pilot server, DS server, the queue |
| **the map** | The key value datastructure, reached by `map_set` and `map_get`. | key value store, the store, key space |
| **time series** | The datastructure `time_series_append` writes. | series alone, metric, stream |
| **counter** | What `counter_get_next_value` hands out, which is how a monitor is elected. | sequence, ticket, lock |
| **key** | One entry in the map, quoted with its prefix. | field, entry, record |

`ds-service` also has **journal** and **mutex** datastructures
that this library does not use.
Do not spend either word on something else.

Two `RuntimeError` messages say "task queue server" in full.
That string is what a user greps for,
so it stays as it is until both messages change in one commit.
Everywhere else, write "the server".

## The search side

Neither Slurm nor `ds-service` has a word here.
These come from this library's own API,
from botorch where the code calls into it,
and from scipy for the design.

| Term | What it names | Source | Do not use |
| --- | --- | --- | --- |
| **study** | One space, its objective and its settings. | Optuna | task, job, problem, experiment |
| **exploration** | What `ExploreSpaceSobolQMC` does. | `num_exploration_points` | sweep, sampling, scan |
| **search** | What `OptimizeSpaceBotorch` does. | `search_parallelism` | optimization as the activity, calibration, tuning |
| **round** | One fit, propose, evaluate cycle. | this library | iteration, phase, wave, generation |
| **design** | The set of points a Sobol' draw produces. | scipy qmc | sample, batch, grid |
| **point** | A parameter assignment, in objective coordinates. | this library | sample, config, trial |
| **unit point** | The same point in the unit cube. | `unit_points` | standardized point, normalized point |
| **candidate** | A point the acquisition proposed and nothing has evaluated. | botorch | proposal as a noun, suggestion |
| **observation** | A point that has been evaluated, with its value. | botorch, GP literature | result, measurement, data point |
| **objective** | The function under study. | this library | target, cost function, model |
| **objective value** | The number under `objective_key`. Lower is better. | `objective_value()` | score, cost, fitness, result |
| **incumbent** | The best value known so far. | BO literature | the best, current best |
| **stalled** | A round that did not beat the incumbent by `min_improvement`. | this library | flat, failed, wasted |
| **results file** | The gzipped pickle `save` writes. | this library | checkpoint, state file, database |

**study**, not task.
A study is not a unit of work.
One study expands into thousands of real tasks,
so calling it a task collides with the one word
that has to stay unambiguous.

**propose** stays as a verb,
and so does `PROPOSE_SECONDS_KEY`.
What it produces is a **candidate**.

**sweep** is retired, in prose and in identifiers alike.
The word is **exploration**, in a tutorial too,
because rule 4 leaves no room for a second word for the same thing.

## Progress, monitoring and logs

| Term | What it names | Source | Do not use |
| --- | --- | --- | --- |
| **progress display** | The JSON object under the `progress_display` key. | the key name | progress bar, progress info |
| **progress series** | The time series `progress:<progress-id>`. | the key name | progress log, history |
| **`desc`** | The label a wait publishes. The published JSON field is `desc`, so that is the spelling everywhere. | the published field | description, title, label |
| **`unit`** | What the count counts. It counts tasks, unless something else is one per task. | this library | item, measure |
| **monitor** | A sampling thread in `monitors.py`. | `monitors.py` | watcher, sampler |
| **sampler** | The callable a monitor calls. | `sample_host`, `CgroupSampler` | reader, probe |
| **subject** | The node or job a monitor samples. | `SubjectInfo` | target, entity, resource |
| **`swtop`** | The program. Call it by name. | the entry point | the monitor, the dashboard, the UI |
| **block** | One of the five lists `swtop` shows: pilot jobs, workers, hosts, slurm jobs and tasks. | `swtop.BlockSpec`, `swtop_widgets.BlockTable` | panel, pane, table, widget |
| **tab** | How the `swtop` terminal UI shows a block. | `swtop_widgets.block_pane` | pane, page |
| **error id** | The id in a `RemoteExecutionError`, which appears beside the traceback. | `error_id` | trace id, failure id, error code |
| **time limit** | Slurm's `--time`. | Slurm | walltime, wall time, wall clock |
| **wall clock** | Elapsed real time, as in `acqf_timeout_s`. | ordinary usage | walltime, runtime |

A **monitor** writes a series and `swtop` reads it.
They are opposite ends of one pipe.
Where a document calls `swtop` a monitor,
the section on monitor election becomes unreadable.

## Names that changed

The vocabulary above was applied to the whole repository in one pass.
This table is what moved,
for anyone reading an older branch, an older log file
or a results file written before the change.

| Was | Is |
| --- | --- |
| `define_worker` | `define_job_group` |
| `scale_workers` | `scale_jobs` |
| `WorkerGroup`, `.workers` | `JobGroup`, `.jobs` |
| `ExplorationTask`, `OptimizationTask` | `ExplorationStudy`, `OptimizationStudy` |
| `tasks=` argument and `.tasks` attribute on the two space classes | `studies=` and `.studies` |
| `mapreduce(description=...)` | `mapreduce(desc=...)` |
| `min_search_iterations`, `max_search_iterations` | `min_search_rounds`, `max_search_rounds` |
| `PilotWorkerProcess` | `PilotWorker` |
| `PILOT_WORKER_GROUP`, `PILOT_WORKER_NAME` | `PILOT_JOB_GROUP`, `PILOT_JOB_NAME` |
| `worker_job_info:<worker-name>` | `pilot_job_info:<job-name>` |
| `worker_process_info:<worker-id>` | `worker_info:<worker-id>` |
| `<executor-name>.worker.<group>.<n>` | `<executor-name>.job.<group>.<n>` |
| `swtop` blocks "worker jobs", "worker processes" | "pilot jobs", "workers" |
| `Num tasks:` in a generated sbatch script | `Num Slurm tasks:` |

`PILOT_WORKER_ID` did not change.
It holds a worker id, which is what it always held.

A run started before this change writes the old keys,
so `swtop` from this release shows an empty pilot jobs block against it.
Nothing migrates a server, because the map is in memory
and dies with the server.

## Applying this

**In prose.**
Pick the word from the tables and keep it for the whole document.
Where two senses meet in one paragraph, qualify both,
even where one of them is clear on its own.

**In identifiers.**
A name carries the same word the prose does.
An argument that holds a job group name is `group` or `job_group`,
never `worker`.

**In error messages and CLI help.**
These reach a user who has read nothing else,
so they carry the fullest form:
"job group", "pilot job", "the `ds-service` server".
An error that names an API call names the current one,
whatever this file says the intended name is.

**In a published key or an environment variable.**
The spelling is a contract between a writer and a reader
that ship in the same release but run in different processes.
Change both ends in one commit, and say so in the commit message.

## Related

- [Developer notes](developer-notes.md), for why the code is the way it is
- [The pilot-job model](explanation/pilot-job-model.md),
    which is the same vocabulary written for a user
