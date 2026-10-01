# Computing pi on a Slurm cluster

[<- back to the main README](../../README.md)

In this tutorial we compute $\pi$ by numerical integration
over a quarter of the unit circle, in parallel.
On the way we meet the `ds-service` server, the executor and the workers.
We use `map_reduce`,
the simplest way to spread one function over a pool.
We run on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program is in
[`examples/example_compute_pi.py`](../../examples/example_compute_pi.py).

## Before we start

Run this program from a Rivanna login node.
We need access to the `bii_nssac` account
and to the `bii` partition on Rivanna.
The program names both in `SBATCH_ARGS`.

Follow
[How to install slurm-workflows on Rivanna](../how-to-guides/install-on-rivanna.md)
first.
That guide gives us the two things this program needs:

1. A conda environment with the name `slurm-workflows`,
    which holds the package.
2. The `ds-service` binary on our `PATH`.
    `DsServiceServer` runs it from there.

The example itself lives in this repository.
We clone the repository:

```sh
git clone https://github.com/parantapa/slurm-hpc-workflows.git
cd slurm-hpc-workflows
```

## The arithmetic

$\pi$ is the integral of $4 / (1 + x^2)$ over $[0, 1]$.
We approximate it with a midpoint Riemann sum
over `num_steps` slices of the interval.

The program splits the slices into chunks by stride.
Chunk `i` of `num_chunks` sums slices `i`, `i + num_chunks`,
`i + 2 * num_chunks`, and so on.
No chunk needs anything another chunk computed.
The partial sums add up to the whole.

That shape is a map and a fold.
We map each chunk number to its partial sum,
and we fold the partial sums together with `+`.

## The whole program

```python
from operator import add

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

        with SlurmPilotExecutor("compute-pi", address) as executor:
            executor.define_job_group(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_jobs("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

            # The over-decomposition factor splits the work
            # into more chunks than there are workers.
            # That balances the load when some chunks take longer than others.
            # Ten is a good rule of thumb.
            over_decomp_factor = 10
            num_chunks = NUM_NODES * NTASKS_PER_NODE * over_decomp_factor

            # For item `i`,
            # `do_step_pi` sums every `num_chunks`-th midpoint from `i` on.
            # `add` folds the sums into one.
            total = executor.map_reduce(
                "bii",
                do_step_pi,
                add,
                range(num_chunks),
                0.0,
                desc="compute-pi",
                map_extra_args=(num_steps, num_chunks, stepsize),
            )

    pi = total * stepsize
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

While it works, we open a second shell on the login node.
There we ask Slurm what we hold:

```sh
squeue -u $USER
```

The output looks something like this:

```text
  JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
1846231       bii compute-     user  R       0:42      2 udc-an[28-29]
```

One pilot job is there, named `compute-pi.job.bii.0`
after the executor and the job group.
`squeue` shows only the first 8 characters of that name.
The pilot job moves from `PENDING` to `RUNNING`, and it holds two nodes.
That job is the whole allocation this run gets.

The program enqueued the 800 items, one per chunk,
before a single worker existed.
They waited on the queue until the pilot job started
and its workers claimed them.
When the last task finishes,
the program prints one line.
It looks something like this:

```text
pi = 3.14159265358979...
```

We ran a billion-slice integration
across 80 workers on two compute nodes.
We wrote no `sbatch` script to do it.

[The pilot-job model](../explanation/pilot-job-model.md)
follows every step that happens behind that one job.

## What the program did

Notice `executor.define_job_group(name="bii", ...)`.
It names the job group `bii`,
and gives it the `sbatch` arguments and the setup script.

Notice `executor.scale_jobs("bii", 1)`.
It asks for one pilot job of the `bii` job group.

Notice the call to `executor.map_reduce`.
Its first five arguments are the queue, the function to map,
the function to fold with, the items, and where the fold starts.
The queue is `"bii"`, the name of the job group,
so the workers of that job group do the work.
The items are the chunk numbers, `range(num_chunks)`.

Notice `map_extra_args=(num_steps, num_chunks, stepsize)`.
Each map task calls `do_step_pi(i, num_steps, num_chunks, stepsize)`
for every chunk number `i` it claims.
The extra arguments follow the item.

Notice `add` and `0.0`.
Each map task folds the partial sums of the chunks it claimed.
The fold starts at `0.0`.
One last task, the reduce task, then folds the partial results of the map tasks.

Notice `desc="compute-pi"`.
It labels the progress bar that `swtop` draws for this call.

Notice that `map_reduce` returns the folded value itself.
We multiply it by `stepsize` once, at the end.

## Next steps

[Computing pi with `submit` and `wait`](computing-pi-with-submit.md)
does the same calculation one task at a time,
with the advanced interface.
That interface gives a handle per task, task names, priorities and parent tasks.

[`map`](../reference/map.md)
returns every value instead of one.

[The pilot-job model](../explanation/pilot-job-model.md)
says why the work has this shape.
It also says what each of the three processes does.
