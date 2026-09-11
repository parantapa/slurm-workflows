# Optimizing Himmelblau's function

[<- back to the main README](../../README.md)

This tutorial searches a two-dimensional space
for the minimum of Himmelblau's function,
with `ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch`.
The pool evaluates a whole batch of candidate points per round.
Between rounds, the optimizer fits a Gaussian process.
The model then chooses the next batch.

The example runs on the `bii` partition of the Rivanna cluster at UVA.
It uses the `bii_nssac` account.

The complete program can be found at
[`examples/example_optimize_himmelblau.py`](../../examples/example_optimize_himmelblau.py).

Read [Computing pi on a Slurm cluster](computing-pi.md)
and [Computing pi with a Sobol' QMC sweep](computing-pi-qmc.md)
first.
They cover what this one reuses without further comment.
That is the queue server, the executor, worker groups,
the `sbatch` arguments, and the setup of a sweep.

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
python examples/example_optimize_himmelblau.py
```

Unlike the two pi tutorials, this one needs botorch in two places:

- On the login node, which imports the optimizer.
- In the environment of the workers that run the model fit.

The `botorch` extra of the setup instructions covers both.

## What the search minimizes

```python
def himmelblau(x, y):
    """The objective that the search minimizes over SEARCH_SPACE."""
    value = (x * x + y - 11.0) ** 2 + (x + y * y - 7.0) ** 2
    return {"objective": value, "distance_from_origin": math.hypot(x, y)}
```

Himmelblau's function has four global minima, all with `f = 0`.
Notice that the objective returns a mapping rather than a number.
The search minimizes the `"objective"` entry.
It records every other entry and does not model it.

The run happens in two phases,
because the model needs something to fit before it can choose anything:

1. `ExploreSpaceSobolQMC` sweeps the space and saves what it measured.
2. `OptimizeSpaceBotorch` reads that file and searches on from it.

The second phase then runs in rounds.
Each round has three steps:

1. Fit a model to everything measured so far.
2. Propose a whole batch of points.
3. Evaluate that batch across the pool.

The next round fits the model again.
[About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md)
says why a search has this shape.

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
    """The objective that the search minimizes over SEARCH_SPACE."""
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

Notice `--ntasks-per-node=1 --cpus-per-task=40` in `OPTIMIZER_SBATCH_ARGS`.
That is one worker with the whole node.
But `EVAL_SBATCH_ARGS` asks for 40 slots per node.

The two kinds of work want different nodes.
That is why there are two groups and two queue arguments.
[About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md)
gives the reason.

The two groups also have their own setup scripts.
`opt` is the group that imports botorch on a compute node.
So `OPTIMIZER_SETUP_SCRIPT` must activate an environment that has it.
The example aliases it to `EVAL_SETUP_SCRIPT`, because both are empty there.
That works only if a compute node can import botorch with no setup.

## Phase 1: the sweep

```python
sweep = ExploreSpaceSobolQMC([ExplorationTask(...)], executor)
sweep.run()
sweep.save(EXPLORE_RESULTS)
```

This is the QMC tutorial's program, as the opening move of a search.
It draws 64 Sobol' points over the space.
The `eval` pool evaluates them in one batch.
The model then has something to fit before it makes any decision.

Notice that `EXPLORATION_POINTS = 64` is a literal, not the pool size of 80.
The sweep truncates the count to the nearest lower power of two.
A pool of 80 that asks for 80 points evaluates 64 anyway.
A request for 64 says what will happen.

The seed makes the design repeatable.
The same seed redraws the same starting points.
A different seed explores fresh ground.

`save` writes the file the search reads in phase 2.

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
You hand it the results files, and it models what is in them.
So the task's `name` must be the name the exploration ran under.
After the name, it takes five more positional arguments, in order:

- The space.
- The objective.
- The queue the evaluations go to.
- The queue the fit goes to.
- The batch size.

`SEARCH_PARALLELISM` is that batch size, matched to the pool.
Here it is exactly `NUM_NODES * TASKS_PER_NODE`, so 80.

The optimizer counts the budget in rounds, not points.
Each round fits the model once and evaluates `SEARCH_PARALLELISM` points.
The search stops on whichever comes first,
the ceiling (`MAX_SEARCH_ITERATIONS`) or the early stop.
The early stop needs `PATIENCE` stalled rounds,
once past `MIN_SEARCH_ITERATIONS`.
[`OptimizeSpaceBotorch`](../reference/optimize-space.md#when-it-stops)
describes the four settings.

You do not need to print anything to follow the search.
The optimizer prints the best point after every round.
It also prints how long each fit and each proposal took.

Either phase names every task it submits on the queue server.
So [`swtop`](../how-to-guides/watch-a-run-with-swtop.md) shows
`himmelblau-explore-00` through `himmelblau-search-<round>-<index>`
as the run works through them.
Start it in another shell now.
Then watch a round go by.

## The answer

```python
params, value = opt.best_point(JOB_NAME)
```

Outside both blocks, the executor canceled the pool and shut the queue.
But the optimizer kept every point it evaluated.
So this is an ordinary local value.
`opt.best_output(JOB_NAME)` is the whole mapping the objective returned there,
`distance_from_origin` included.

The program then reports which of the four known minima it landed nearest.
Which one that is depends on the seed.
All four are equally good.
The search settles on whichever its batches reached first.

## Next steps

- [How to resume a search](../how-to-guides/resume-a-search.md)
    carries a search on across a walltime limit,
    which is what the two saved files are for.
- [`OptimizeSpaceBotorch`](../reference/optimize-space.md) is the full API:
    several spaces searched at once,
    how the search decides to stop, and the acquisition settings.
- [Search spaces](../reference/search-space.md) covers integer,
    categorical and log-scaled parameters.
- [About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md)
    says why the search has this shape,
    and when it is worth its overhead.
