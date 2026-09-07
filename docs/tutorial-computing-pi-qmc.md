# Tutorial: Computing PI with a Sobol' QMC sweep

[<- back to the main README](../README.md)

This tutorial computes $\pi$ a second time,
with `ExploreSpaceSobolQMC` instead of hand-written `submit` calls.
The sweep draws a low-discrepancy design over a space,
evaluates every point of it across the pilot pool,
and keeps what came back,
so the driver never touches a task handle.
This example uses the `bii` partition of the Rivanna cluster at UVA,
and uses the `bii_nssac` account.

The complete program can be found at
[`examples/example_compute_pi_qmc.py`](../examples/example_compute_pi_qmc.py).

Read [Tutorial: Computing PI on a Slurm Cluster](tutorial-computing-pi.md) first.
It covers what this one reuses without further comment:
the queue server, the executor, the worker group,
and what the `sbatch` arguments mean.

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

The design is drawn with scipy,
so nothing here needs botorch or torch,
on the login node or on the workers.

## The arithmetic

A quarter of the unit circle has area $\pi / 4$,
so a point of the unit square lands inside it with probability $\pi / 4$.
Scoring a point 4 when it is inside and 0 when it is outside
makes the mean score over the design an estimate of $\pi$.

The points are a scrambled Sobol' sequence rather than a random sample,
which covers the square more evenly at a given count:
4096 points land about `0.003` from $\pi$,
where 4096 uniform random points average about `0.02`.

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

Everything up to `scale_workers` is the first tutorial's program.
What happens after it, in order:

* the sweep draws 4096 Sobol' points over `SAMPLE_SPACE`;
* every point is submitted to the `bii` queue as one task,
    named `compute-pi-qmc-explore-0000` and up,
    so the sweep can be followed in [`swtop`](how-to-use-swtop.md);
* `run` blocks until all of them are back;
* `save` writes the points, the scores and the whole outputs to a file;
* the driver averages the scores.

## The sweep

**The space** is one entry per objective argument,
keyed by the argument's name.
`FloatRange(0.0, 1.0)` is the unit interval;
`IntRange`, `CategoricalRange` and log-scaled `FloatRange`s exist too,
and one space may mix all four.

**The point count** is truncated down to a power of two,
because that is where a Sobol' sequence is balanced.
Ask for 5000 and 4096 are evaluated,
so it is worth asking for a power of two.

**The seed** is optional.
Given, it redraws exactly the same design;
left out, one is drawn from `os.urandom` and printed
so the run can still be repeated.
`sweep.design(JOB_NAME)` returns the points without evaluating them.

**The objective** runs on a compute node, once per point,
and its argument names have to match the keys of the space.
It returns a mapping rather than a number:
`objective_key` names the entry that is the value
(here `"score"`, the default being `"objective"`),
and every other entry is recorded alongside it,
which is where a runtime or an intermediate metric goes.

**`ExploreSpaceSobolQMC` takes a list of tasks.**
A second space would be swept in the same batch rather than after this one,
and each task may name its own queue.
Constructing it submits nothing:
it validates the tasks first,
so a misspelled keyword or an empty space
is reported before anything reaches the cluster.

**Failures are collected, not raised at the first one.**
The whole batch is waited for, what came back is kept,
and the error then names the tasks whose evaluations failed.

## The results

`sweep.results[JOB_NAME]` holds four index-aligned lists:
`points`, `values`, `outputs` and `unit_points`.
`values` is the `objective_key` entry of each result, in submission order,
so the mean of it is the estimate.

The sweep prints a line of its own naming the lowest value it saw.
Ignore it here:
the ranking is there because a sweep usually precedes a search,
and this program wants the mean of the design rather than the best point.

`sweep.save` writes a gzipped pickle of the points, the values and the outputs.
`unit_points` is not in the file:
it is derived from the space, so a reader recomputes it.
`load_results`, also from `slurm_workflows`, reads it back.

## A note on scale

One evaluation here is two multiplications,
which is not what the sweep is for:
it exists for objectives costing seconds or minutes,
where a queue round trip per point is noise.
Two nodes are 80 workers and about 50 points each,
and a wider pool would not help:
the queue round trips, not the arithmetic, are the run.

## Next steps

[Tutorial: Optimizing Himmelblau's function](tutorial-optimize-himmelblau.md)
takes a file like the one this run saved and searches on from it
with `OptimizeSpaceBotorch`,
choosing where to evaluate next
instead of drawing every point up front.
