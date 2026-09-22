# Computing pi with a Sobol' QMC exploration

[<- back to the main README](../../README.md)

In this tutorial we compute $\pi$ again,
this time with `ExploreSpaceSobolQMC`,
which owns the submit-and-wait loop we wrote by hand before.
The exploration draws a low-discrepancy design over a space,
evaluates every point of that design across a pool of workers,
and keeps what came back.

We run on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi_qmc.py`](../../examples/example_compute_pi_qmc.py).

## Before we start

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
A Sobol' sequence gives low-discrepancy points for Quasi Monte Carlo methods.

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


def inside_quarter_circle(x, y):
    """Score one sample point: 4 inside the quarter circle, 0 outside."""
    radius = math.hypot(x, y)
    return {"score": 4.0 if radius <= 1.0 else 0.0, "radius": radius}


def main():
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

While it works, we open a second shell on the login node
and point [`swtop`](../how-to-guides/watch-a-run-with-swtop.md)
at the address the driver gave the executor:

```sh
swtop 10.0.0.1:5051
```

We watch the `ready` count fall from 4096 toward zero,
as the workers claim the points and post what the objective returned.

What to watch for while it runs, in order:

* The `ds-service` server starts on the login node.
* The executor submits one pilot job across `NUM_NODES` nodes.
* `srun` starts a worker on every Slurm task in that job.
    Each worker connects back to the server over InfiniBand.
* The exploration draws 4096 Sobol' points over `SAMPLE_SPACE`.
    It submits every point to the `bii` queue as a task.
* The exploration names the tasks `compute-pi-qmc-explore-0000` and up,
    so each point is recognizable in the tasks block.
* `run` blocks until every task is back.
* `save` writes the points, the objective values and the whole outputs to a file.
* The executor cancels the pilot job at the end of its block.
* The driver averages the objective values.

Notice where the last three lines of the program sit.
We read `exploration.results` after both `with` blocks close.
The server is gone by then, and the pilot job is canceled.
The points and the objective values are still there,
as ordinary local values in our own process.

One thing outlives the run, and we look at it now:

```sh
ls -lh compute-pi-qmc.pkl.gz
```

That results file holds every point of the design,
with the whole mapping the objective returned for it.
[Optimizing Himmelblau's function](optimizing-himmelblau.md)
searches on from a file of that kind.

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

[Computing pi on a Slurm cluster](computing-pi.md)
computes the same number the other way round.
It submits each piece of work itself with `submit` and `wait`.
If the work is not a function over a space, copy that shape.

[`ExploreSpaceSobolQMC`](../reference/explore-space.md) is the full API
for an exploration: the objective contract, the methods,
and the results file `save` writes.
