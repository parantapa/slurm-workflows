# What a run publishes

[<- back to the main README](../../README.md)

A run leaves a trail on the `ds-service` server and in its work dir.
The executor writes some of it, each worker writes some of it,
and the monitors write the rest.
[`swtop`](swtop.md) is what reads it back.

The environment variables a task sees are in [`SlurmPilotExecutor`](executor.md#the-environment-a-task-sees).

## Keys and time series

### Pilot job and worker keys

**The executor publishes each pilot job as it submits it**,
under `pilot_job_info:<job-name>`, as a JSON object:

| Field | Value |
| --- | --- |
| `name` | The pilot job's name, which is also its Slurm job name |
| `group` | The job group whose queue it will serve |
| `slurm_job_id` | The job `sbatch` returned |
| `submit_time` | When it was submitted, an ISO 8601 timestamp with an offset |

**Each worker publishes where it runs when it starts**,
under `worker_info:<worker-id>`,
where the worker id is `<job-name>.<job-id>.<hostname>.<pid>`.
The value is a JSON object, not a pickle,
so anything can read it:

| Field | Value |
| --- | --- |
| `group` | The job group whose queue it serves |
| `name` | The name of the pilot job it runs in, which is that job's Slurm job name |
| `slurm_job_id` | The job it is running in |
| `hostname` | The compute node it landed on |
| `pid` | Its process id on that node |
| `restart_generation` | The value of the `restart_generation:<group>` counter when it started |
| `start_time` | When it started, an ISO 8601 timestamp with an offset |

The worker id is what the worker claims tasks under.
`task_get_worker_id` says which worker holds a running task.
The worker id is the path from a task to the worker and the node that ran it.
Nothing removes the key when a worker exits.

**Each pilot job and each worker publishes when it starts and when it exits**,
in keys of their own.
Each value is a JSON object with one field,
an ISO 8601 timestamp with an offset:

| Key | Field | Written |
| --- | --- | --- |
| `pilot_job_start:<job-name>` | `start_time` | When Slurm starts the pilot job's batch script |
| `pilot_job_exit:<job-name>` | `exit_time` | When that batch script exits |
| `worker_exit:<worker-id>` | `exit_time` | When the worker exits |

A worker's start time is the `start_time` field of `worker_info:<worker-id>`.

The batch script publishes the pilot job's two keys
by running its worker script with `--pilot-job-event start`
and `--pilot-job-event exit`.
That run sources the job group's setup script as a worker does,
and then publishes, and starts no worker.

A pilot job or a worker that Slurm cancels
or that reaches its time limit still publishes its exit.
Slurm sends it SIGTERM first,
and it publishes before Slurm sends SIGKILL.
A worker that fails to build its actor publishes its exit too,
and so does a worker that restarts.
A process that dies of SIGKILL, or with its node, publishes nothing.
Nothing removes these keys either.

### `mapreduce` and `map` tasks

**A `mapreduce` or `map` call publishes one task per item**,
on a queue of its own, under `<executor-name>.mapreduce.`
or `<executor-name>.map.`.
Those tasks outlive the call.
[`mapreduce`](mapreduce.md) and [`map`](map.md) cover the ids and what they hold.

### Task names and actor arguments

**The executor also writes three kinds of key**.
`task_name:<task-id>` holds the UTF-8 name that `set_task_name` gives a task.
`actor_class_args:<group>` and `actor_class_kwargs:<group>` hold the cloudpickled constructor arguments
of a job group's actor.
[`Task`](executor.md#task)
and [`define_job_group` options](executor.md#define_job_group-options)
describe them.

### The restart counter

**The executor counts the restarts of each job group**
in the counter `restart_generation:<group>`.
[`restart_jobs`](executor.md#restart_jobs) adds one to it,
and returns the new value.
A counter that does not exist reads as 0.
Each worker reads it when it starts,
before it publishes `worker_info:<worker-id>`,
and records the value there as `restart_generation`.

A worker that sees the counter move past that value
publishes its exit after its current task, and exits.
The worker script then starts a new worker in its place.
The new worker has a new process id,
so it publishes a `worker_info:` key of its own,
under a new worker id.

### Host, job and GPU series

Workers also sample the node they run on, the Slurm job they belong to,
and the GPUs that job can see.
Every 5 seconds they append to these `ds-service` time series:

- `host_free_memory:<hostname>`
- `host_load_average:<hostname>`
- `host_dev_shm_used:<hostname>`
- `host_tmp_used:<hostname>`
- `slurm_job_memory:<job-id>:<hostname>`
- `slurm_job_cpu:<job-id>:<hostname>`

On a node where the job can see a GPU,
the same worker also samples each GPU it can see.
It appends to these time series,
where `<gpu>` is the index NVML gives the GPU, as `nvidia-smi` shows it:

| Series | Value |
| --- | --- |
| `slurm_job_gpu_memory_used:<job-id>:<hostname>:<gpu>` | GPU memory in use, in bytes |
| `slurm_job_gpu_memory_free:<job-id>:<hostname>:<gpu>` | GPU memory available, in bytes |
| `slurm_job_gpu_utilization:<job-id>:<hostname>:<gpu>` | Percent of the driver's last sample period, 1/6 s to 1 s, in which a kernel ran on the GPU |

The type of each GPU is text, which a time series cannot hold.
So it goes in the map, under `slurm_job_gpu_info:<job-id>:<hostname>:<gpu>`,
as a JSON object:

| Field | Value |
| --- | --- |
| `name` | The GPU's type, such as `NVIDIA A100-SXM4-80GB` |
| `uuid` | The GPU's UUID |
| `memory_total` | The GPU's memory, in bytes, or `null` where the GPU does not report it |

The worker writes this key when it first sees the GPU,
and again only if a field changes.
A measurement the GPU does not support,
such as the utilization of a MIG instance,
has no points in its series.

The worker reads the GPUs through NVML, the NVIDIA driver's library,
with the `nvidia-ml-py` package.
A node with no NVIDIA driver, or where NVML lists no GPU,
publishes none of these keys.
NVML ignores `CUDA_VISIBLE_DEVICES`.
On a cluster that constrains devices through cgroups,
it lists the GPUs of the job's step on that node.
On a cluster that does not, it lists every GPU on the node,
including those of other jobs.

One worker per job does this on each node the job runs on.
It samples the node, the part of the job on that node,
and the GPUs that part can see.
The workers elect it with the `host_monitor:<hostname>:<job-id>:<generation>` counter,
where `<generation>` is the worker's `restart_generation`.
Two pilot jobs that share a node therefore both sample it,
into the same host series.
After a restart, the new workers elect a new monitor.
Until the worker that won the old election restarts,
it samples the node as well, into the same series.

[`swtop`](swtop.md) displays the host and job series.
It does not display the GPU series yet.

Why a run publishes in these two halves rather than one
is in [The trail a run leaves](../explanation/the-trail-a-run-leaves.md).
That page also says why nothing updates
`pilot_job_info:` and `worker_info:` keys after the first write.

## Watching a wait

`as_completed` and `wait` both require `desc`,
and `unit` names what the call counts.
Neither call prints a progress bar of its own.
They publish what they work through to the server,
where [`swtop`](swtop.md) draws it.

Each call writes the key `progress_display`, a JSON object with these fields:

| Field | Value |
| --- | --- |
| `progress_id` | A fresh UUID4, one per call |
| `desc` | The `desc` given to the call |
| `unit` | The `unit` given to the call |
| `total` | How many tasks were handed in |

Each call also appends the number of tasks that came back so far
to the time series `progress:<progress_id>`.
The series opens at 0 and closes at the number that returned.
The call appends the count at most once a second while tasks arrive.

The next call overwrites the key.
The server therefore holds the display for the most recent wait,
and the series holds the history of each.

## Logs

Everything for a run lives under the executor's `work_dir`.
The executor prints that path at startup,
and the attribute `executor.work_dir` holds it:

| File | Contents |
| --- | --- |
| `executor.log` | Pilot job submission, and cancellation by `scale_jobs`, from the executor's side. A line for each `mapreduce` or `map` call that enqueues items. A line for each `restart_jobs` call, and one for the end of its wait. Each liveness check that could not run `squeue`. |
| `<job-name>.sh`, `<job-name>.sbatch` | The generated scripts |
| `<job-name>-<job-id>-<rank>.out` | One per Slurm task, shared by each worker a restart starts in it: setup-script output, task-by-task progress, full tracebacks |
| `<job-name>-<job-id>.out` | The pilot job's own output, and the worker's log too when the job holds a single Slurm task or runs a batch worker |

`<job-name>` is `<executor-name>.job.<group>.<index>`,
which is the Slurm job name, so `squeue` shows which run a job belongs to.
The work dir itself defaults to the path
that [`SlurmPilotExecutor`](executor.md#slurmpilotexecutorname-server_address-work_dirnone) gives.

Slurm writes those files.
The worker does not redirect its own output.
Which of the two holds a worker's log depends on the job group's definition:

- **`is_batch_worker=False`** (the default) runs the worker under `srun`,
    which fans out over every Slurm task in the allocation.
    Each Slurm task gets `--output <work-dir>/<job-name>-%j-%t.out`,
    so `<rank>` is the task rank.
    That file is the worker's log.
    `<job-name>-<job-id>.out` then holds
    only what the batch script itself emitted,
    which in practice means `srun`'s own errors.

    The exception is a job of exactly one Slurm task:
    `--ntasks=1`, or a single node and no other Slurm task count.
    It keeps `srun` but drops the `--output`
    and writes to `<job-name>-<job-id>.out` like a batch worker.
    The count is per *job*, not per node:
    `--nodes=4 --ntasks-per-node=1` is four Slurm tasks
    and still gets four per-Slurm-task files.

    `<job-name>-<job-id>.out` records which way a job went.
    It records the Slurm task count the job decided on (`Num Slurm tasks: 4`),
    says so when it redirects,
    and traces the `srun` command it ran.
- **`is_batch_worker=True`** runs one worker directly on the batch host,
    with no `srun` and so no per-Slurm-task file.
    Everything lands in `<job-name>-<job-id>.out`.

The `error_id` inside a `RemoteExecutionError`
appears verbatim next to the traceback in the worker's log.

## Related

- [`SlurmPilotExecutor`](executor.md)
- [`swtop`](swtop.md)
- [The trail a run leaves](../explanation/the-trail-a-run-leaves.md)
