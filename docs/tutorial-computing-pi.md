# Tutorial: Computing PI on a Slurm Cluster

[<- back to the main README](../README.md)

This tutorial demonstrates how to use the `slurm-workflows` package.
It numerically computes $\pi$
by integrating over a quarter of the unit circle in parallel.
The example runs on the `bii` partition of the Rivanna cluster at UVA,
and uses the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi.py`](../examples/example_compute_pi.py).

## Before you start

This program is meant to be run from a Rivanna login node.

Work through
[Installation and setup on Rivanna](installation-and-setup-instructions-for-rivanna.md)
first.
It leaves you with the two things this program needs:

1. **A conda environment named `slurm-workflows`**,
    with the package and its `botorch` extra installed in it.
2. **The `ds-service` binary on your `PATH`.**
    `DsServiceServer` runs it from there.

The example itself lives in this repository, so clone it:

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

$\pi$ is the integral of $4 / (1 + x^2)$ over $[0, 1]$,
approximated here by a midpoint Riemann sum
over `num_steps` slices of the interval.

The slices are split between tasks by stride:
task `i` of `num_tasks` sums slices `i`, `i + num_tasks`, `i + 2 * num_tasks`,
and so on.
No task needs anything another task computed,
and the partial sums they return add up to the whole.

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
    """Sum every `step`-th midpoint slice, beginning at `start`."""
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

What happens when you run it, in order:

* a `ds-service` task queue starts on the login node;
* one Slurm job is submitted, spanning `NUM_NODES` nodes;
* `srun` starts a worker process on every task slot in that job;
* each worker connects back to the queue over InfiniBand,
    pulls tasks, runs them, and posts results;
* the driver blocks in `wait()` until every task is back.

## Next steps

[Tutorial: Computing PI with a Sobol' QMC sweep](tutorial-computing-pi-qmc.md)
does the same calculation with `ExploreSpaceSobolQMC`,
which owns the submit-and-wait loop
and keeps what every evaluation returned.
