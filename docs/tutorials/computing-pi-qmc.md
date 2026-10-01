# Computing pi with a Sobol' QMC exploration

[<- back to the main README](../../README.md)

In this tutorial we compute $\pi$ again,
this time with `ExploreSpaceSobolQMC`.
That class owns the submit-and-wait loop
we wrote by hand before.
The exploration draws a low-discrepancy design over a space,
evaluates every point of that design across a pool of workers,
and keeps what came back.

We run on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program is in
[`examples/example_compute_pi_qmc.py`](../../examples/example_compute_pi_qmc.py).

## Before we start

Work through [Computing pi on a Slurm cluster](computing-pi.md) first.
It computes the same number with `submit` and `wait` calls of its own.

Run this program from a Rivanna login node.
Follow
[How to install slurm-workflows on Rivanna](../how-to-guides/install-on-rivanna.md)
first.
Next, we clone this repository:

```sh
git clone https://github.com/parantapa/slurm-hpc-workflows.git
cd slurm-hpc-workflows
```

## The arithmetic

A quarter of the unit circle has area $\pi / 4$.
So a point of the unit square lands inside it with probability $\pi / 4$.
We evaluate an objective that is 4 inside the circle and 0 outside it.
The mean objective value over the design is then an estimate of $\pi$.

The exploration draws the points from a scrambled Sobol' sequence,
not from uniform sampling.
A Sobol' sequence gives low-discrepancy points
for quasi-Monte Carlo (QMC) methods.

## The whole program

```python
import math

from ds_service_client import DsServiceServer
from slurm_workflows import (
    ExplorationStudy,
    ExploreSpaceSobolQMC,
    FloatRange,
    SlurmPilotExecutor,
)

SETUP_SCRIPT = ""

NUM_NODES = 2
NTASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={NTASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]

RUN_NAME = "compute-pi-qmc"
RESULTS_FILE = "compute-pi-qmc.pkl.gz"

NUM_SAMPLE_POINTS = 4096
SEED = 20260907

SAMPLE_SPACE = {
    "x": FloatRange(0.0, 1.0),
    "y": FloatRange(0.0, 1.0),
}


def inside_quarter_circle(x: float, y: float) -> dict[str, float]:
    """Score one sample point: 4 inside the quarter circle, 0 outside."""
    radius = math.hypot(x, y)
    return {"score": 4.0 if radius <= 1.0 else 0.0, "radius": radius}


def main() -> None:
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor(RUN_NAME, address) as executor:
            executor.define_job_group(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_jobs("bii", 1)

            exploration = ExploreSpaceSobolQMC(
                [
                    ExplorationStudy(
                        name=RUN_NAME,
                        space=SAMPLE_SPACE,
                        objective=inside_quarter_circle,
                        objective_queue="bii",
                        num_exploration_points=NUM_SAMPLE_POINTS,
                        seed=SEED,
                        objective_key="score",
                    )
                ],
                executor,
            )

            exploration.run()
            exploration.save(RESULTS_FILE)

    scores = exploration.results[RUN_NAME].values
    pi = sum(scores) / len(scores)
    print(f"pi = {pi} (from {len(scores)} sample points)")


if __name__ == "__main__":
    main()
```

## Run it

We run the program from the root of that clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_compute_pi_qmc.py
```

The program does not print the server address.
The executor prints its work directory when it starts.
The line looks something like this:

```text
work directory: '/home/<user>/.cache/slurm-workflows/compute-pi-qmc/2026-10-01T09:30:00.123456'
```

We open a second shell on the login node.
There we read the server address
from the worker script `compute-pi-qmc.job.bii.0.sh` in that directory:

```sh
grep -- --server-address '<work directory>/compute-pi-qmc.job.bii.0.sh'
```

```text
        --server-address '10.0.0.1:5051' \
```

We point [`swtop`](../how-to-guides/watch-a-run-with-swtop.md)
at that address:

```sh
swtop 10.0.0.1:5051
```

We watch the `ready` count fall from 4096 toward zero,
as the workers claim the points and post what the objective returned.

Behind that count, these steps happen, in order:

* The server, the pilot job and the workers start
    as in [Computing pi on a Slurm cluster](computing-pi.md).
* The exploration draws 4096 Sobol' points over `SAMPLE_SPACE`.
    It submits every point to the `bii` queue as a task.
* The exploration names the tasks `compute-pi-qmc-explore-0000` and up,
    so each point is recognizable in the tasks block.
* `run` blocks until every task is back.
* `run` then prints the study's best point.
    The best point has the lowest score,
    so here it is a point outside the circle.
* `save` writes the points, the objective values and the whole mappings to a file.
* The executor cancels the pilot job at the end of its block.
* The driver averages the objective values.

Notice where the last three lines of the program sit.
We read `exploration.results` after both `with` blocks close.
The server is gone by then, and the pilot job is canceled.
The points and the objective values are still there,
as ordinary local values in our own process.

The program prints `pi = `, then the estimate,
then `(from 4096 sample points)`.

One thing outlives the run, and we look at it now:

```sh
ls -lh compute-pi-qmc.pkl.gz
```

That results file holds every point of the design,
with the whole mapping the objective returned for it.

An estimate of the same number came back,
and we wrote no `submit` or `wait` call to get it.
We described a space and an objective,
and the exploration did the submitting, the waiting and the bookkeeping.

## Next steps

[Optimizing Himmelblau's function](optimizing-himmelblau.md)
takes a file like the one this run saved.
It then searches on from that file with `OptimizeSpaceBotorch`.
The optimizer chooses where to evaluate next.
It does not draw every point up front.

[`ExploreSpaceSobolQMC`](../reference/explore-space.md) is the full API
for an exploration: the objective contract, the methods,
and the results file `save` writes.
