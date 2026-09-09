# Tutorial: Computing PI with a Sobol' QMC sweep

[<- back to the main README](../README.md)

This tutorial computes $\pi$ with `ExploreSpaceSobolQMC`,
which owns the submit-and-wait loop:
the sweep draws a low-discrepancy design over a space,
evaluates every point of it across a pool of pilot workers,
and keeps what came back.
The example runs on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi_qmc.py`](../examples/example_compute_pi_qmc.py).

## Before you start

This program is meant to be run from a Rivanna login node,
set up as in
[Installation and setup on Rivanna](installation-and-setup-instructions-for-rivanna.md),
with this repository cloned:

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

A quarter of the unit circle has area $\pi / 4$,
so a point of the unit square lands inside it with probability $\pi / 4$.
Scoring a point 4 when it is inside and 0 when it is outside
makes the mean score over the design an estimate of $\pi$.

The points are generated using a scrambled Sobol' sequence ---
a method for generating low-discrepancy points for Quasi Monte Carlo methods ---
rather than using uniform sampling.

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

What happens when you run it, in order:

* a `ds-service` task queue starts on the login node;
* one Slurm job is submitted, spanning `NUM_NODES` nodes;
* `srun` starts a worker process on every task slot in that job,
    and each connects back to the queue over InfiniBand;
* the sweep draws 4096 Sobol' points over `SAMPLE_SPACE`
    and submits every one of them to the `bii` queue as a task,
    named `compute-pi-qmc-explore-0000` and up,
    so the sweep can be followed in [`swtop`](how-to-use-swtop.md);
* `run` blocks until all of them are back;
* `save` writes the points, the scores and the whole outputs to a file;
* leaving the executor's block cancels the pilot job;
* the driver averages the scores.

## Next steps

[Tutorial: Optimizing Himmelblau's function](tutorial-optimize-himmelblau.md)
takes a file like the one this run saved and searches on from it
with `OptimizeSpaceBotorch`,
choosing where to evaluate next
instead of drawing every point up front.

[Tutorial: Computing PI on a Slurm Cluster](tutorial-computing-pi.md)
computes the same number the other way round,
submitting each piece of work itself with `submit` and `wait`,
which is what to copy when the work is not a function over a space.
