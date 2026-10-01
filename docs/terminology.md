# Terminology

[<- back to the main README](../README.md)

This file names the words this project uses, one per concept.

This file binds prose and identifiers alike.
It covers the docstrings, the comments, the error messages and the CLI help.
It covers every document under `docs/` and the README,
and the names of classes, methods, arguments and published keys.

A reader of this library holds three vocabularies at once,
because a run spans three systems.
Where they disagree with each other, this file says which sense wins
and how to spell the other one.

## Where the words come from

The words come from three sources, in this order of precedence:

1. **Slurm**, for anything that exists on the cluster.
    Slurm named it.
    This library does not get a vote.
2. **`ds-service`**, for anything that exists on the server.
    The client API is the spelling, down to the case.
3. **This library's own public API**,
    where neither of the first two has a word for the thing.

Do not invent a word where a source already decided.
Do not spend a source's word on something that source does not mean by it.

Two facts from `ds_service_client` settle the hard cases.
This section states them before the tables:

* `ds_service_client` documents `task_get(worker_id, queue)`
  as "Claim a task for `worker_id` from the first queue holding one".
  In `ds-service` a **worker** is whatever claims a task.
  Here the claimant is the process,
  because `PilotWorker` passes its own `worker_id`.
  A worker is therefore a process, and nothing else is a worker.
* `ds_service_client` documents `task_add`
  as "Register a task, and enqueue it on each of its queues once it is Ready".
  A **task** is the `ds-service` unit of work.
  Slurm also calls a process inside a job step a task.
  That second sense always carries the word Slurm in front of it.

## The four rules

1. **Bare "task" means a `ds-service` task.**
    Every other sense is qualified, every time.
2. **The driver is a process.
    The executor is an object.**
    If a sentence stays true with two executors in one program,
    it is about the driver.
3. **A worker is a process.
    A pilot job is a Slurm job.
    A job group is a group of pilot jobs.**
4. **One name per thing.**
    Where a paragraph wants a second word for variety, it repeats the first.

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
| **restart generation** | The value of the `restart_generation:<group>` counter. `restart_jobs` adds one to it, and each worker records the value it read at startup. A restart replaces the worker, so a new worker has a new worker id. | this library | epoch, version, restart count |
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

The **executor** submits, waits, scales and cancels.
It has a `name`, a `work_dir` and a log of its own.
It is what a `with` block closes,
and it is what the rule of one executor per server is about.

For example:

- "the driver runs on a login node"
- "the executor cancels every pilot job when the block ends"
- "`map` puts the values in input order on the driver"
- "two executors on one server share a queue namespace"

### Job group, not worker group

A job group is a recipe for **pilot jobs**.
`scale_jobs` sets how many pilot jobs a job group has.
The job group holds `SlurmJob` objects.
Two settings decide how many workers those jobs start:
the Slurm task count in the sbatch arguments of the job group,
and `is_batch_worker`.
The job group does not control that number.
One job group scaled to a single job can hold eighty workers.

Write **job group** in full, never bare "group".
Slurm's own "group" is the Unix group in `--gid`.

### A queue is never a partition

The two pi examples name a job group `bii`,
after the partition its jobs run on.
That name is legal.
But it reads as though the job group and the partition were the same thing.
Name a job group after what its workers do (`cpu`, `gpu`, `loader`),
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
| **queue** | A named `ds-service` queue. Equal to a job group name, except for an item queue. | `ds-service` | channel, topic, the server, partition |
| **submit** | What the executor does with a task, and what `sbatch` does with a pilot job. Always name the object. | both | push, post, launch a task |
| **enqueue** | What `task_add` does to a task on each of its queues. | `ds-service` | add, register, queue up |
| **claim** | What a worker does to a task through `task_get`. | `ds-service` | pull, take, fetch, grab |
| **task output** | What `task_done` recorded, which `Task.output` holds. | `ds-service` (`output`) | result, return value, answer |
| **setup script** | The shell text inlined into the generated worker script. | this library | env script, prologue, bootstrap |
| **work dir** | The executor's directory of scripts and logs. | `work_dir` | run directory, output directory |
| **simple interface** | `map` and `map_reduce`: one call runs one function over a whole iterable and blocks until it is done. | this library | high-level API, easy mode, convenience functions |
| **advanced interface** | `submit`, with `wait` or `as_completed`: one call per task, and a `Task` handle for each. | this library | low-level API, raw interface, expert mode |

### `map_reduce` and `map`

One `map_reduce` or `map` call has two kinds of task and one kind of item.
Name all three.

| Term | What it names | Do not use |
| --- | --- | --- |
| **item** | One element of `iterable`. | task, unit, record |
| **item task** | The task that carries one item, `<queue>.item.<i>`. It holds no function. | item alone, map_reduce task |
| **item queue** | `<executor-name>.map_reduce.<n>.<token>`, or `<executor-name>.map.<n>.<token>` for `map`. No job group serves it. | map_reduce queue, the private queue |
| **map task** | The task that claims item tasks and maps them, with the task name `<item-queue>.task.<i>`. Under `map_reduce` it also folds them. | map_reduce task, folding task, worker task |
| **partial result** | What one map task of `map_reduce` returns. | partial, shard, chunk result |
| **reduce task** | The one task of a `map_reduce` call that folds the partial results, with the task name `<item-queue>.reduce`. Its parents are the map tasks. | final task, combine task, driver-side fold |
| **chunk** | Several items batched into one item, to amortize the round trip. | group, batch, block |
| **fold** | Applying `reduce_fn`. One verb for the fold in a map task and in the reduce task alike. "Reduce" names only the reduce task, never the act. | reduce as a verb, accumulate, combine |

## States

Take these verbatim from whichever source owns them.
The two sources disagree on the spelling of one word,
and that is not a typo to correct.

| States | Owner | Note |
| --- | --- | --- |
| `Waiting`, `Ready`, `Running`, `Finished`, `Failed`, `Canceled`, `Undefined` | `ds-service` `TaskState` | One `l` in `Canceled`. |
| `PENDING`, `RUNNING`, `COMPLETED`, `CANCELLED`, `TIMEOUT` | Slurm job states | Two `l`s in `CANCELLED`. |

These terms name a task as a wait sees it:

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

The server is one process, and it has one name.

| Term | What it names | Do not use |
| --- | --- | --- |
| **the `ds-service` server**, short form **the server** | The one process everything talks through. | queue server, task queue server, task-queue server, pilot server, DS server, the queue |
| **the map** | The key-value data structure, reached by `map_set` and `map_get`. | key value store, the store, key space |
| **time series** | The data structure `time_series_append` writes. | series alone, metric, stream |
| **counter** | What `counter_get_next_value` returns, which is how the workers elect a monitor and how `restart_jobs` requests a restart. | sequence, ticket, lock |
| **key** | One entry in the map, quoted with its prefix. | field, entry, record |

`ds-service` also has **journal** and **mutex** data structures
that this library does not use.
Do not spend either word on something else.

Two `RuntimeError` messages say "task queue server" in full.
That string is what a user greps for,
so it stays as it is until both messages change in one commit.
Everywhere else, write "the server".

## Progress, monitoring and logs

| Term | What it names | Source | Do not use |
| --- | --- | --- | --- |
| **progress display** | The JSON object under the `progress_display` key. | the key name | progress bar, progress info |
| **progress series** | The time series `progress:<progress-id>`. | the key name | progress log, history |
| **`desc`** | The label a wait publishes. The published JSON field is `desc`, so that is the spelling everywhere. | the published field | description, title, label |
| **`unit`** | What the count counts. It counts tasks, unless something else is one per task. | this library | item, measure |
| **monitor** | A sampling thread in `monitors.py`. | `monitors.py` | watcher, sampler |
| **sampler** | The callable a monitor calls. | `sample_host`, `CgroupSampler` | reader, probe |
| **subject** | The node, the job on one node, or one GPU of that job, that a monitor samples. | `SubjectInfo`, `gpu_subject` | target, entity, resource |
| **`swtop`** | The program. Call it by name. | the entry point | the monitor, the dashboard, the UI |
| **block** | One of the five lists `swtop` shows: pilot jobs, workers, hosts, slurm jobs and tasks. | `swtop.BlockSpec`, `swtop_widgets.BlockTable` | panel, pane, table, widget |
| **tab** | How the `swtop` terminal UI shows a block. | `swtop_widgets.block_pane` | pane, page |
| **error id** | The id in a `RemoteExecutionError`, which appears beside the traceback. | `error_id` | trace id, failure id, error code |
| **time limit** | Slurm's `--time`. | Slurm | walltime, wall time, wall clock |
| **wall clock** | Elapsed real time, as the `timeout` of `restart_jobs` counts it. | ordinary usage | walltime, runtime |

A **monitor** writes a time series and `swtop` reads it.
A monitor and `swtop` are opposite ends of one pipe.
Where a document calls `swtop` a monitor,
the section on monitor election becomes unreadable.

## Identifiers that keep a retired word

`PILOT_WORKER_ID` did not change.
It holds a worker id, which is what it always held.

Some identifiers kept a retired word.
The worker's logger is still named `worker_process`,
and that name appears in every worker log line.
`swtop.Snapshot` still holds the pilot jobs in its `worker_jobs` field.
`MAP_REDUCE_ITEM_TEMPLATE` and `MAP_REDUCE_TOKEN_LEN` keep `map_reduce` in their names,
although they serve `map` too.
The test suite keeps more of them in its test names,
such as `TestDefineWorker` and `TestScaleWorkers`.

## How to apply these terms

**In prose.**
Pick the word from the tables.
Keep it for the whole document.
Where two senses meet in one paragraph, qualify both,
even where one of them is clear on its own.

**In identifiers.**
A name carries the same word the prose does.
An argument that holds a job group name is `group` or `job_group`,
or `name` on `define_job_group` and `scale_jobs`.
Those two methods act on a job group.
Such an argument is never `worker`.

**In error messages and CLI help.**
Error messages and CLI help reach a user who read nothing else,
so they carry the fullest form:
"job group", "pilot job", "the `ds-service` server".
An error that names an API call names the current one,
whatever this file says the intended name is.

**In a published key or an environment variable.**
The spelling is a contract between a writer and a reader
that ship in the same release but run in different processes.
Change both ends in one commit.
Say so in the commit message.

## Related

- [Developer notes](developer-notes.md), for why the code is the way it is
- [The pilot-job model](explanation/pilot-job-model.md),
    which is the same vocabulary written for a user
