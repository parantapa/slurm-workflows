# Computing pi on a Slurm cluster

[<- back to the main README](../../README.md)

In this tutorial we compute $\pi$ by numerical integration
over a quarter of the unit circle, in parallel.
On the way we meet the server, the executor and the workers.
We run on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi.py`](../../examples/example_compute_pi.py).

## Before we start

Run this program from a Rivanna login node.

Follow
[How to install slurm-workflows on Rivanna](../how-to-guides/install-on-rivanna.md)
first.
That guide gives us the two things this program needs:

1. A conda environment named `slurm-workflows`,
    with the package installed in it.
2. The `ds-service` binary on our `PATH`.
    `DsServiceServer` runs it from there.

The example itself lives in this repository.
We clone it:

```sh
git clone https://github.com/parantapa/slurm-hpc-workflows.git
cd slurm-hpc-workflows
```

## The arithmetic

$\pi$ is the integral of $4 / (1 + x^2)$ over $[0, 1]$.
We approximate it with a midpoint Riemann sum
over `num_steps` slices of the interval.

The program splits the slices between the tasks by stride.
Task `i` of `num_pi_tasks` sums slices `i`, `i + num_pi_tasks`,
`i + 2 * num_pi_tasks`, and so on.
No task needs anything another task computed.
The partial sums they return add up to the whole.

## The whole program

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

SETUP_SCRIPT = ""

NUM_NODES = 2
NTASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={NTASKS_PER_NODE} --cpus-per-task=1 --mem=0",
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
            executor.define_job_group(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_jobs("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

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

## Run it

We run the program from the root of that clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_compute_pi.py
```

While it works, we open a second shell on the login node
and ask Slurm what we hold:

```sh
squeue -u $USER
```

One job is there, named `compute-pi.job.bii.0`,
after the executor and the job group.
It moves from `PENDING` to `RUNNING`, and it holds two nodes.
That job is the whole allocation this run gets.

What to watch for while it runs, in order:

* The `ds-service` server starts on the login node.
* The executor submits one Slurm job across `NUM_NODES` nodes.
* `srun` starts a worker on every Slurm task in that job.
* Each worker connects back to the server over InfiniBand.
    It claims tasks, runs them, and posts results.
* The driver blocks in `wait()` until every task is back.

Notice that the program submits the 800 tasks before a single worker exists.
The tasks wait on the queue until a pilot job starts and its workers claim them.
We time nothing by hand.

We ran a thousand-million-slice integration
across 80 workers on two compute nodes,
and we wrote no sbatch script to do it.
Every later program in this documentation has the shape of this one.

## Next steps

[Computing pi with a Sobol' QMC exploration](computing-pi-qmc.md)
does the same calculation with `ExploreSpaceSobolQMC`.
That class owns the submit-and-wait loop
and keeps what every evaluation returned.

[About the pilot-job model](../explanation/about-the-pilot-job-model.md)
says why the work has this shape.
It also says what each of the three processes does.
