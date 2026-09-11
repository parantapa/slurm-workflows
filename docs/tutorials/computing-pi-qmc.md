# Computing pi with a Sobol' QMC sweep

[<- back to the main README](../../README.md)

This tutorial computes $\pi$ with `ExploreSpaceSobolQMC`.
That class owns the submit-and-wait loop.
The sweep draws a low-discrepancy design over a space.
It evaluates every point of the design across a pool of pilot workers.
Then it keeps what came back.

The example runs on the `bii` partition of the Rivanna cluster at UVA.
It uses the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi_qmc.py`](../../examples/example_compute_pi_qmc.py).

## Before you start

Run this program from a Rivanna login node.
Follow
[How to install slurm-workflows on Rivanna](../how-to-guides/install-on-rivanna.md)
first.
Next, clone this repository:

```sh
git clone https://github.com/parantapa/slurm-hpc-workflows.git
cd slurm-hpc-workflows
```

Then run it from the root of that clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_compute_pi_qmc.py
```

## The arithmetic

A quarter of the unit circle has area $\pi / 4$.
So a point of the unit square lands inside it with probability $\pi / 4$.
The program scores a point 4 inside the circle and 0 outside it.
The mean score over the design is then an estimate of $\pi$.

The sweep draws the points from a scrambled Sobol' sequence,
not from uniform sampling.
A Sobol' sequence gives low-discrepancy points for Quasi Monte Carlo methods.

## The whole program

```python
import math

from ds_service_client import DsServiceServer
from slurm_workflows import (
    ExplorationTask,
    ExploreSpaceSobolQMC,
    FloatRange,
    SlurmPilotExecutor,
)

SETUP_SCRIPT = ""

NUM_NODES = 2
TASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={TASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]

JOB_NAME = "compute-pi-qmc"
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

        with SlurmPilotExecutor(JOB_NAME, address) as executor:
            executor.define_worker(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_workers("bii", 1)

            sweep = ExploreSpaceSobolQMC(
                [
                    ExplorationTask(
                        name=JOB_NAME,
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

            sweep.run()
            sweep.save(RESULTS_FILE)

    scores = sweep.results[JOB_NAME].values
    pi = sum(scores) / len(scores)
    print(f"pi = {pi} (from {len(scores)} sample points)")


if __name__ == "__main__":
    main()
```

What to watch for while it runs, in order:

* A `ds-service` task queue starts on the login node.
* The executor submits one Slurm job across `NUM_NODES` nodes.
* `srun` starts a worker process on every task slot in that job.
    Each worker connects back to the queue over InfiniBand.
* The sweep draws 4096 Sobol' points over `SAMPLE_SPACE`.
    It submits every point to the `bii` queue as a task.
* The sweep names the tasks `compute-pi-qmc-explore-0000` and up,
    so you can follow them in [`swtop`](../how-to-guides/watch-a-run-with-swtop.md).
* `run` blocks until every task is back.
* `save` writes the points, the scores and the whole outputs to a file.
* The executor cancels the pilot job at the end of its block.
* The driver averages the scores.

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
for a sweep: the objective contract, the methods,
and the results file `save` writes.
