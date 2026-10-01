# Computing pi with `submit` and `wait`

[<- back to the main README](../../README.md)

In this tutorial we compute $\pi$ again,
with the same arithmetic as [Computing pi on a Slurm cluster](computing-pi.md).
This time we submit one task per chunk ourselves, and wait for them.
`submit` with `wait` is the advanced interface of the executor.
It gives us a handle for every task,
so we can name tasks, order them, chain them, and decide what a failure does.

The complete program is in
[`examples/example_compute_pi_submit.py`](../../examples/example_compute_pi_submit.py).

## Before we start

Finish [Computing pi on a Slurm cluster](computing-pi.md) first.
This tutorial uses the same environment, the same clone,
and the same arithmetic.
Here, task `i` of `num_pi_tasks` sums the slices that chunk `i` summed there.

## The whole program

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

SETUP_SCRIPT = ""

NUM_NODES = 2
# A node of the `bii` partition has 40 cores.
NTASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={NTASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]


def do_step_pi(start: int, stop: int, step: int, stepsize: float) -> float:
    """Sum 4 / (1 + x*x) at every `step`-th midpoint, unscaled by `stepsize`."""
    x, s = 0.0, 0.0
    for i in range(start, stop, step):
        x = (i + 0.5) * stepsize
        s += 4.0 / (1.0 + x * x)
    return s


def main() -> None:
    # `ib0` is the InfiniBand interface, which the compute nodes can reach.
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor("compute-pi-submit", address) as executor:
            executor.define_job_group(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_jobs("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

            # The over-decomposition factor splits the work
            # into more tasks than there are workers.
            # That balances the load when some tasks take longer than others.
            # Ten is a good rule of thumb.
            over_decomp_factor = 10
            num_pi_tasks = NUM_NODES * NTASKS_PER_NODE * over_decomp_factor

            tasks = []
            for i in range(num_pi_tasks):
                task = executor.submit(
                    "bii",
                    do_step_pi,
                    start=i,
                    stop=num_steps,
                    step=num_pi_tasks,
                    stepsize=stepsize,
                )
                executor.set_task_name(task, f"task-{i:04d}")
                tasks.append(task)

            executor.wait(tasks, desc="compute-pi", unit="task")

    pi = sum(task.output for task in tasks) * stepsize
    print(f"pi = {pi}")


if __name__ == "__main__":
    main()
```

Notice that `executor.submit("bii", do_step_pi, ...)` returns a `Task` handle at once.
The task runs `do_step_pi` with the keyword arguments we pass.
Nothing blocks here.

Notice `executor.set_task_name(task, f"task-{i:04d}")`.
It gives each task a name, from `task-0000` up.

Notice that `executor.wait(tasks, ...)` blocks until every task finishes.

Notice that `task.output` holds the task output of each task after `wait`.
Here that is the partial sum that `do_step_pi` returned.
We add the partial sums ourselves, on the driver.

## Run it

We run the program from the root of the clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_compute_pi_submit.py
```

This time the pilot job has the name `compute-pi-submit.job.bii.0`,
after the executor and the job group.
When the last task finishes, the program prints the same line as before:

```text
pi = 3.14159265358979...
```

We summed a billion slices with 800 tasks
that we submitted, named and waited on ourselves.

## Next steps

[`submit`, `wait` and `as_completed`](../reference/submit-and-wait.md)
shows what else the handles give us:
a priority, parent tasks, a policy for failures, and results as they arrive.
It lists every option, the `Task` fields,
and the errors that end a wait.

[How to watch a run with `swtop`](../how-to-guides/watch-a-run-with-swtop.md)
shows a run like this one in `swtop`,
with each task under the name we gave it.

[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md)
loads an expensive object once per worker,
for `submit` and for `map` alike.

[The pilot-job model](../explanation/pilot-job-model.md)
says which interface to use.
