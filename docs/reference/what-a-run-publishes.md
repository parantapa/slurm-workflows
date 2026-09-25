# What a run publishes

[<- back to the main README](../../README.md)

A run leaves a trail on the `ds-service` server and in its work dir.
The executor writes some of it, each worker writes some of it,
and the monitors write the rest.
[`swtop`](swtop.md) is what reads it back.

## The environment a task sees

Inside a task, these environment variables exist:

- `PILOT_JOB_NAME`, the pilot job's name, for example `demo.job.cpu.0`
- `PILOT_JOB_GROUP`, the job group name
- `PILOT_WORKER_ID`, the id the worker claims tasks under
- `DS_SERVER_ADDRESS`, the server address
- plus the usual Slurm variables (`SLURM_JOB_ID`, ...)

The worker sets the first four when it starts,
before it builds its actor and before it claims a task.

## Keys and time series

**The executor publishes each pilot job as it submits it**,
under `pilot_job_info:<job-name>`, as a JSON object:

| Field | Value |
| --- | --- |
| `name` | The pilot job's name, which is also its Slurm job name |
| `group` | The job group whose queue it will serve |
| `slurm_job_id` | The job `sbatch` returned |
| `submit_time` | When it was submitted, an ISO 8601 timestamp with an offset |

**A `mapreduce` call publishes one task per item**,
on a queue of its own, under `<executor-name>.mapreduce.`.
Those tasks outlive the call.
[`mapreduce`](mapreduce.md) covers the ids and what they hold.

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
| `start_time` | When it started, an ISO 8601 timestamp with an offset |

The worker id is what the worker claims tasks under
(`task_get_worker_id` says which worker holds a running task).
It is the path from a task to the worker and the node that ran it.
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
A worker that fails to build its actor publishes its exit too.
A process that dies of SIGKILL, or with its node, publishes nothing.
Nothing removes these keys either.

Workers also sample the node they run on and the Slurm job they belong to.
Every 5 seconds they append to these `ds-service` time series:

- `host_free_memory:<hostname>`
- `host_load_average:<hostname>`
- `host_dev_shm_used:<hostname>`
- `host_tmp_used:<hostname>`
- `slurm_job_memory:<job-id>`
- `slurm_job_cpu:<job-id>`

One worker per job does this for the job,
and one worker per job does it for each node the job runs on.
The workers elect them with the `host_monitor:<hostname>:<job-id>`
and `slurm_job_monitor:<job-id>` counters.
Two pilot jobs that share a node therefore both sample it,
into the same host series.
[`swtop`](swtop.md) displays the result.

Why a run publishes in these two halves rather than one
is in [The trail a run leaves](../explanation/the-trail-a-run-leaves.md).
That page also says why nothing updates these two kinds of key after the first write.

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
| `executor.log` | Pilot job submission and cancellation from the executor's side, a line for each `mapreduce` call, and each liveness check that could not run `squeue` |
| `<job-name>.sh`, `<job-name>.sbatch` | The generated scripts |
| `<job-name>-<job-id>-<rank>.out` | One per worker: setup-script output, task-by-task progress, full tracebacks |
| `<job-name>-<job-id>.out` | The pilot job's own output, and the worker's log too when the job holds a single Slurm task or runs a batch worker |

`<job-name>` is `<executor-name>.job.<group>.<index>`,
which is the Slurm job name, so `squeue` shows which run a job belongs to.
The work dir itself defaults to the path that
[`SlurmPilotExecutor`](executor.md#slurmpilotexecutorname-server_address-work_dirnone) gives.

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

    The batch file records which way a job went.
    It opens with the Slurm task count the job decided on (`Num Slurm tasks: 4`),
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
