# Tutorial: Optimizing Himmelblau's function

[<- back to the main README](../README.md)

This tutorial searches a two-dimensional space for the minimum of
Himmelblau's function,
with `ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch`.
The pool evaluates a whole batch of candidate points per round,
and a Gaussian process fitted between rounds chooses the next batch.
The example runs on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program can be found at
[`examples/example_optimize_himmelblau.py`](../examples/example_optimize_himmelblau.py).

Read [Tutorial: Computing PI on a Slurm Cluster](tutorial-computing-pi.md)
and [Tutorial: Computing PI with a Sobol' QMC sweep](tutorial-computing-pi-qmc.md)
first.
They cover what this one reuses without further comment:
the queue server, the executor, worker groups,
the `sbatch` arguments, and how a sweep is set up.

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
python examples/example_optimize_himmelblau.py
```

Unlike the two pi tutorials, this one needs botorch:
on the login node, which imports the optimizer,
and in the environment of the workers that run the model fit.
The `botorch` extra of the setup instructions covers both.

## What is being optimized

```python
def himmelblau(x, y):
    """The objective, minimized over SEARCH_SPACE."""
    value = (x * x + y - 11.0) ** 2 + (x + y * y - 7.0) ** 2
    return {"objective": value, "distance_from_origin": math.hypot(x, y)}
```

Himmelblau's function has four global minima, all with `f = 0`,
which is what makes it a fair test:
a search that only ever walks downhill from where it started
finds whichever minimum it happened to begin near.

The objective runs on a compute node, once per point,
and its argument names have to match the keys of `SEARCH_SPACE`.
It returns a mapping rather than a number.
`"objective"` is the entry that is **minimized**
(negate a score you would rather maximize),
and every other entry is recorded but not modelled,
which is where a runtime or an intermediate metric goes.

The arithmetic here is microseconds.
Bayesian optimization earns its overhead when one evaluation costs minutes;
below that, the model fits dominate the runtime.

## Why the run has rounds

The two pi programs knew every task up front and submitted them in one go.
A search cannot: which point is worth trying next
depends on what the previous points returned.

So the run is a sequence of **rounds**.
Each round fits a Gaussian process to everything measured so far,
asks it for a whole batch of points at once,
evaluates that batch across the pool, and refits.
The batch is what keeps the pool busy:
a one-point-at-a-time optimizer would leave all but one worker idle.

That takes two phases, because a model needs something to fit
before it can choose anything:

1. `ExploreSpaceSobolQMC` sweeps the space and saves what it measured.
2. `OptimizeSpaceBotorch` is handed that file and searches on from it.

## The whole program

```python
import math

from ds_service_client import DsServiceServer
from slurm_workflows import (
    ExplorationTask,
    ExploreSpaceSobolQMC,
    FloatRange,
    OptimizationTask,
    OptimizeSpaceBotorch,
    SlurmPilotExecutor,
)

EVAL_SETUP_SCRIPT = ""
OPTIMIZER_SETUP_SCRIPT = EVAL_SETUP_SCRIPT

NUM_NODES = 2
TASKS_PER_NODE = 40

EVAL_SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={TASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]

OPTIMIZER_SBATCH_ARGS = [
    "--account=bii_nssac",
    "--partition=bii --nodes=1",
    "--ntasks-per-node=1 --cpus-per-task=40 --mem=0",
    "--time=1:00:00",
]

JOB_NAME = "himmelblau"

EXPLORE_RESULTS = "himmelblau-explore.pkl.gz"
SEARCH_RESULTS = "himmelblau-search.pkl.gz"

SEED = 20260730

SEARCH_SPACE = {
    "x": FloatRange(-5.0, 5.0),
    "y": FloatRange(-5.0, 5.0),
}

EXPLORATION_POINTS = 64

SEARCH_PARALLELISM = NUM_NODES * TASKS_PER_NODE

MIN_SEARCH_ITERATIONS = 5
MAX_SEARCH_ITERATIONS = 30
PATIENCE = 3
MIN_IMPROVEMENT = 0.05

# Himmelblau's function has four global minima, all with f = 0.
KNOWN_MINIMA = [
    (3.0, 2.0),
    (-2.805118, 3.131312),
    (-3.779310, -3.283186),
    (3.584428, -1.848126),
]


def himmelblau(x, y):
    """The objective, minimized over SEARCH_SPACE."""
    value = (x * x + y - 11.0) ** 2 + (x + y * y - 7.0) ** 2
    return {"objective": value, "distance_from_origin": math.hypot(x, y)}


def main():
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor(JOB_NAME, address) as executor:
            executor.define_worker(
                name="eval",
                sbatch_args=EVAL_SBATCH_ARGS,
                setup_script=EVAL_SETUP_SCRIPT,
            )
            executor.define_worker(
                name="opt",
                sbatch_args=OPTIMIZER_SBATCH_ARGS,
                setup_script=OPTIMIZER_SETUP_SCRIPT,
            )

            executor.scale_workers("eval", 1)
            executor.scale_workers("opt", 1)

            sweep = ExploreSpaceSobolQMC(
                [
                    ExplorationTask(
                        JOB_NAME,
                        SEARCH_SPACE,
                        himmelblau,
                        "eval",
                        EXPLORATION_POINTS,
                        SEED,
                    )
                ],
                executor,
            )

            print(
                f"\n=== exploration: {sweep.tasks[0].num_exploration_points}"
                f" points, one batch ==="
            )
            sweep.run()
            sweep.save(EXPLORE_RESULTS)

            opt = OptimizeSpaceBotorch(
                [
                    OptimizationTask(
                        JOB_NAME,
                        SEARCH_SPACE,
                        himmelblau,
                        "eval",
                        "opt",
                        SEARCH_PARALLELISM,
                        min_search_iterations=MIN_SEARCH_ITERATIONS,
                        max_search_iterations=MAX_SEARCH_ITERATIONS,
                        patience=PATIENCE,
                        min_improvement=MIN_IMPROVEMENT,
                    )
                ],
                executor,
                [EXPLORE_RESULTS],
            )

            print(
                f"\n=== search: up to {MAX_SEARCH_ITERATIONS} rounds"
                f" of {SEARCH_PARALLELISM} points ==="
            )
            opt.run()
            opt.save(SEARCH_RESULTS)

    params, value = opt.best_point(JOB_NAME)
    nearest = min(
        KNOWN_MINIMA,
        key=lambda m: (m[0] - params["x"]) ** 2 + (m[1] - params["y"]) ** 2,
    )
    print(f"\nbest f = {value:.6g} (true minimum is 0)")
    print(f"  full result: {opt.best_output(JOB_NAME)}")
    print(f"  found at x = {params['x']:.4f}, y = {params['y']:.4f}")
    print(f"  nearest known minimum: x = {nearest[0]:.4f}, y = {nearest[1]:.4f}")


if __name__ == "__main__":
    main()
```

## Two worker groups

```python
executor.define_worker(name="eval", ...)
executor.define_worker(name="opt", ...)
```

The objective evaluations go to the `eval` pool, one worker per task slot.
The model fit and the acquisition optimization go to the `opt` pool,
as one task per round.

Two groups rather than one, because the two want different nodes.
An evaluation is a cheap single-threaded call, 80 at a time;
the fit is a single task that wants cores and memory,
and grows more expensive every round as the model does.
Hence `--ntasks-per-node=1 --cpus-per-task=40` in `OPTIMIZER_SBATCH_ARGS`:
one worker with the whole node,
since torch threads the fit's linear algebra
and a second worker there would sit idle all run.

`optimizer_queue="eval"` would also work,
since the fit and the evaluations never run at the same time,
but the fit would then wait for a slot in a pool sized for the objective.

The two groups also have their own setup scripts.
`opt` is the one that imports botorch on a compute node,
so `OPTIMIZER_SETUP_SCRIPT` has to activate an environment that has it.
It is aliased to `EVAL_SETUP_SCRIPT` in the example
because both are empty there,
which works only if botorch is importable on a compute node
without doing anything.

## Phase 1: the sweep

```python
sweep = ExploreSpaceSobolQMC([ExplorationTask(...)], executor)
sweep.run()
sweep.save(EXPLORE_RESULTS)
```

This is the QMC tutorial's program, as the opening move of a search:
64 Sobol' points over the space, evaluated in one batch on `eval`,
to give the model something to fit before it starts making decisions.

`EXPLORATION_POINTS = 64` is a literal rather than the pool size,
because the count is truncated down to a power of two.
A pool of 80 asking for 80 points would evaluate 64
and leave 16 idle for the whole sweep without saying so.
Asking for 64 says what will happen.

The seed is what makes the design repeatable:
the same seed redraws the same starting points,
a different one explores fresh ground.
Left out, a seed is drawn and printed
so the run can still be repeated afterwards.

`save` is what the search reads, and what makes the run resumable:
the pool can die here and the search still has its points.

## Phase 2: the search

```python
opt = OptimizeSpaceBotorch(
    [OptimizationTask(JOB_NAME, SEARCH_SPACE, himmelblau, "eval", "opt", ...)],
    executor,
    [EXPLORE_RESULTS],
)
opt.run()
```

The optimizer never explores.
It is handed the results files and models what is in them,
so the task's `name` has to be the name the exploration ran under.
Its positional arguments are the space, the objective,
the queue the evaluations go to, the queue the fit goes to,
and the batch size.

`SEARCH_PARALLELISM` is that batch size, so match it to the pool:
a bigger batch queues behind the workers,
a smaller one leaves workers idle.
Here it is exactly `NUM_NODES * TASKS_PER_NODE`, so 80.

The budget is counted in **rounds**, not points.
Each round fits the model once and evaluates `SEARCH_PARALLELISM` points,
and the search stops on whichever comes first,
the ceiling or the early stop:

| Setting | Effect |
| --- | --- |
| `MIN_SEARCH_ITERATIONS` | Rounds that always run, so a slow start is not mistaken for a finished search |
| `MAX_SEARCH_ITERATIONS` | Hard ceiling, reached even while still improving |
| `PATIENCE` | Consecutive stalled rounds that end the search; an improving round resets the count |
| `MIN_IMPROVEMENT` | Fraction a round must beat the incumbent by to count as improving; `0.05` is 5% |

Stalled rounds below the floor still count towards `PATIENCE`
but cannot be the round that stops the search,
so the earliest stop is `max(MIN_SEARCH_ITERATIONS, PATIENCE)` rounds.

Each round is a barrier: fit, propose a batch, evaluate, refit.
That is the cost of choosing a batch jointly,
and the reason a round should be as wide as the pool.
No progress reporting is needed in the driver:
the optimizer prints the best point after every round,
plus how long each fit and each proposal took.
A best that stops moving while the fits keep growing
means the budget is going to the model rather than to the search.

Every task either phase submits is named on the queue server,
so [`swtop`](how-to-use-swtop.md) shows
`himmelblau-explore-00` through `himmelblau-search-<round>-<index>`
as the run works through them.

## Resuming a search

```python
opt.save(SEARCH_RESULTS)
```

This holds only the points this run evaluated,
so a later run passes both files and counts each point once:

```python
OptimizeSpaceBotorch(tasks, executor, [EXPLORE_RESULTS, SEARCH_RESULTS])
```

That is how a search stopped by a walltime limit
carries on in the next job.

## The answer

```python
params, value = opt.best_point(JOB_NAME)
```

Outside both blocks the pool is cancelled and the queue is gone,
but the optimizer kept every point it evaluated,
so this is an ordinary local value.
`opt.best_output(JOB_NAME)` is the whole mapping the objective returned there,
`distance_from_origin` included.

The program then reports which of the four known minima it landed nearest.
Which one that is depends on the seed:
all four are equally good, and the search settles on whichever
its batches reached first.

## Next steps

[Reference](reference.md) is the full API:
integer, categorical and log-scaled parameters,
several spaces searched at once,
what a round does and how the search decides to stop,
and the acquisition settings.
