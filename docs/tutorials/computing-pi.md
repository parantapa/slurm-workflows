# Computing pi on a Slurm cluster

[<- back to the main README](../../README.md)

This tutorial shows how to use the `slurm-workflows` package.
It computes $\pi$ by numerical integration
over a quarter of the unit circle, in parallel.
The example runs on the `bii` partition of the Rivanna cluster at UVA.
It uses the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi.py`](../../examples/example_compute_pi.py).

## Before you start

Run this program from a Rivanna login node.

Follow
[How to install slurm-workflows on Rivanna](../how-to-guides/install-on-rivanna.md)
first.
That guide gives you the two things this program needs:

1. A conda environment named `slurm-workflows`,
    with the package and its `botorch` extra installed in it.
2. The `ds-service` binary on your `PATH`.
    `DsServiceServer` runs it from there.

The example itself lives in this repository.
Clone it:

```sh
git clone https://github.com/parantapa/slurm-hpc-workflows.git
cd slurm-hpc-workflows
```

Then run it from the root of that clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_compute_pi.py
```

## The arithmetic

$\pi$ is the integral of $4 / (1 + x^2)$ over $[0, 1]$.
This program approximates it with a midpoint Riemann sum
over `num_steps` slices of the interval.

The program splits the slices between tasks by stride.
Task `i` of `num_tasks` sums slices `i`, `i + num_tasks`, `i + 2 * num_tasks`,
and so on.
No task needs anything another task computed.
The partial sums they return add up to the whole.

## The whole program

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

SETUP_SCRIPT = ""

NUM_NODES = 2
TASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={TASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]


def do_step_pi(start, stop, step, stepsize):
    """Sum every `step`-th midpoint slice, from `start`."""
    x, s = 0.0, 0.0
    for i in range(start, stop, step):
        x = (i + 0.5) * stepsize
        s += 4.0 / (1.0 + x * x)
    return s


def main():
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor("compute-pi", address) as executor:
            executor.define_worker(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_workers("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

            over_decomp_factor = 10
            num_tasks = NUM_NODES * TASKS_PER_NODE * over_decomp_factor

            tasks = []
            for i in range(num_tasks):
                task = executor.submit(
                    "bii",
                    do_step_pi,
                    start=i,
                    stop=num_steps,
                    step=num_tasks,
                    stepsize=stepsize,
                )
                executor.set_task_name(task, f"task-{i:04d}")
                tasks.append(task)

            executor.wait(tasks, desc="compute-pi", unit="slice")

    pi = sum(task.output for task in tasks) * stepsize
    print(f"pi = {pi}")


if __name__ == "__main__":
    main()
```

What to watch for while it runs, in order:

* A `ds-service` task queue starts on the login node.
* The executor submits one Slurm job across `NUM_NODES` nodes.
    `squeue -u $USER` shows it.
* `srun` starts a worker process on every task slot in that job.
* Each worker connects back to the queue over InfiniBand.
    It pulls tasks, runs them, and posts results.
* The driver blocks in `wait()` until every task is back.

Notice that the program submits the 800 tasks before a single worker exists.
The tasks wait on the queue until a pilot job starts and takes them.
You time nothing by hand.

## Next steps

[Computing pi with a Sobol' QMC sweep](computing-pi-qmc.md)
does the same calculation with `ExploreSpaceSobolQMC`.
That class owns the submit-and-wait loop
and keeps what every evaluation returned.

[About the pilot-job model](../explanation/about-the-pilot-job-model.md)
says why the work has this shape.
It also says what each of the three processes does.
