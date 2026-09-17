# Optimizing Himmelblau's function

[<- back to the main README](../../README.md)

In this tutorial we search a two-dimensional space
for the minimum of Himmelblau's function,
with `ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch`.
The pool of workers evaluates a whole batch of candidate points per round.
Between rounds, the optimizer fits a Gaussian process,
and the model then chooses the next batch.

We run on the `bii` partition of the Rivanna cluster at UVA,
under the `bii_nssac` account.

The complete program can be found at
[`examples/example_optimize_himmelblau.py`](../../examples/example_optimize_himmelblau.py).

We read [Computing pi on a Slurm cluster](computing-pi.md)
and [Computing pi with a Sobol' QMC exploration](computing-pi-qmc.md)
first.
They cover what this one reuses without further comment.
That is the `ds-service` server, the executor, job groups,
the `sbatch` arguments, and the setup of an exploration.

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

Unlike the two pi tutorials, this one needs botorch in two places:

- On the login node, which imports the optimizer.
- In the environment of the workers that run the model fit.

The `botorch` extra of the setup instructions covers both.

## What the search minimizes

Himmelblau's function has four global minima, all with `f = 0`.
Notice that the objective returns a mapping rather than a number.
The search minimizes the `"objective"` entry.
It records every other entry and does not model it.

The run happens in two phases,
because the model needs something to fit before it can choose anything:

1. `ExploreSpaceSobolQMC` explores the space and saves what it measured.
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
    ExplorationStudy,
    ExploreSpaceSobolQMC,
    FloatRange,
    OptimizationStudy,
    OptimizeSpaceBotorch,
    SlurmPilotExecutor,
)

EVAL_SETUP_SCRIPT = ""
OPTIMIZER_SETUP_SCRIPT = EVAL_SETUP_SCRIPT

NUM_NODES = 2
NTASKS_PER_NODE = 40

EVAL_SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={NTASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]

OPTIMIZER_SBATCH_ARGS = [
    "--account=bii_nssac",
    "--partition=bii --nodes=1",
    "--ntasks-per-node=1 --cpus-per-task=40 --mem=0",
    "--time=1:00:00",
]

RUN_NAME = "himmelblau"

EXPLORE_RESULTS = "himmelblau-explore.pkl.gz"
SEARCH_RESULTS = "himmelblau-search.pkl.gz"

SEED = 20260730

SEARCH_SPACE = {
    "x": FloatRange(-5.0, 5.0),
    "y": FloatRange(-5.0, 5.0),
}

EXPLORATION_POINTS = 64

SEARCH_PARALLELISM = NUM_NODES * NTASKS_PER_NODE

MIN_SEARCH_ROUNDS = 5
MAX_SEARCH_ROUNDS = 30
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

        with SlurmPilotExecutor(RUN_NAME, address) as executor:
            executor.define_job_group(
                name="eval",
                sbatch_args=EVAL_SBATCH_ARGS,
                setup_script=EVAL_SETUP_SCRIPT,
            )
            executor.define_job_group(
                name="opt",
                sbatch_args=OPTIMIZER_SBATCH_ARGS,
                setup_script=OPTIMIZER_SETUP_SCRIPT,
            )

            executor.scale_jobs("eval", 1)
            executor.scale_jobs("opt", 1)

            exploration = ExploreSpaceSobolQMC(
                [
                    ExplorationStudy(
                        RUN_NAME,
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
                f"\n=== exploration: {exploration.studies[0].num_exploration_points}"
                f" points, one batch ==="
            )
            exploration.run()
            exploration.save(EXPLORE_RESULTS)

            opt = OptimizeSpaceBotorch(
                [
                    OptimizationStudy(
                        RUN_NAME,
                        SEARCH_SPACE,
                        himmelblau,
                        "eval",
                        "opt",
                        SEARCH_PARALLELISM,
                        min_search_rounds=MIN_SEARCH_ROUNDS,
                        max_search_rounds=MAX_SEARCH_ROUNDS,
                        patience=PATIENCE,
                        min_improvement=MIN_IMPROVEMENT,
                    )
                ],
                executor,
                [EXPLORE_RESULTS],
            )

            print(
                f"\n=== search: up to {MAX_SEARCH_ROUNDS} rounds"
                f" of {SEARCH_PARALLELISM} points ==="
            )
            opt.run()
            opt.save(SEARCH_RESULTS)

    params, value = opt.best_point(RUN_NAME)
    nearest = min(
        KNOWN_MINIMA,
        key=lambda m: (m[0] - params["x"]) ** 2 + (m[1] - params["y"]) ** 2,
    )
    print(f"\nbest f = {value:.6g} (true minimum is 0)")
    print(f"  full result: {opt.best_output(RUN_NAME)}")
    print(f"  found at x = {params['x']:.4f}, y = {params['y']:.4f}")
    print(f"  nearest known minimum: x = {nearest[0]:.4f}, y = {nearest[1]:.4f}")


if __name__ == "__main__":
    main()
```

## Run it

We run the program from the root of that clone:

```sh
module load miniforge/26.3.2
conda activate slurm-workflows
python examples/example_optimize_himmelblau.py
```

Either phase names every task it submits on the server.
So [`swtop`](../how-to-guides/watch-a-run-with-swtop.md) shows
`himmelblau-explore-00` through `himmelblau-search-<round>-<index>`
as the run works through them.
We start it in another shell now,
and watch a round go by.

The run takes a while.
The rest of this tutorial reads the program while it works.

## Two job groups

```python
executor.define_job_group(name="eval", ...)
executor.define_job_group(name="opt", ...)
```

The objective evaluations go to the `eval` pool, one worker per Slurm task.
The model fit and the acquisition optimization go to the `opt` pool,
as one task per round.

Notice `--ntasks-per-node=1 --cpus-per-task=40` in `OPTIMIZER_SBATCH_ARGS`.
That is one worker with the whole node.
But `EVAL_SBATCH_ARGS` asks for 40 Slurm tasks per node.

The two kinds of work want different nodes.
That is why there are two job groups and two queue arguments.
[About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md)
gives the reason.

The two job groups also have their own setup scripts.
`opt` is the job group that imports botorch on a compute node.
So `OPTIMIZER_SETUP_SCRIPT` must activate an environment that has it.
The example aliases it to `EVAL_SETUP_SCRIPT`.
Both are empty there,
because on Rivanna a compute node imports botorch with no setup.
On a cluster where that is not true,
`OPTIMIZER_SETUP_SCRIPT` is where the environment gets activated.

## Phase 1: the exploration

```python
exploration = ExploreSpaceSobolQMC([ExplorationStudy(...)], executor)
exploration.run()
exploration.save(EXPLORE_RESULTS)
```

This is the QMC tutorial's program, as the opening move of a search.
It draws 64 Sobol' points over the space.
The `eval` pool evaluates them in one batch.
The model then has something to fit before it makes any decision.

Notice that `EXPLORATION_POINTS = 64` is a literal, not the pool size of 80.
The exploration truncates the count to the nearest lower power of two.
A pool of 80 that asks for 80 points evaluates 64 anyway.
A request for 64 says what will happen.

The seed makes the design repeatable.
The same seed redraws the same starting points.
A different seed explores fresh ground.

`save` writes the file the search reads in phase 2.

## Phase 2: the search

```python
opt = OptimizeSpaceBotorch(
    [OptimizationStudy(RUN_NAME, SEARCH_SPACE, himmelblau, "eval", "opt", ...)],
    executor,
    [EXPLORE_RESULTS],
)
opt.run()
```

The optimizer never explores.
We hand it the results files, and it models what is in them.
So the study's `name` must be the name the exploration ran under.

The two queue arguments after it are the `eval` and `opt` job groups
we defined above.
`SEARCH_PARALLELISM` is the batch size, matched to the pool.
Here it is exactly `NUM_NODES * NTASKS_PER_NODE`, so 80.
[`OptimizeSpaceBotorch`](../reference/optimize-space.md)
has the rest of the signature.

The search runs at most `MAX_SEARCH_ROUNDS` rounds,
and probably fewer,
because it stops early once it stops improving.
[`OptimizeSpaceBotorch`](../reference/optimize-space.md#when-it-stops)
says what decides that.

We print nothing to follow the search.
The optimizer prints the best point after every round.
It also prints how long each fit and each propose step took.

## The answer

```python
params, value = opt.best_point(RUN_NAME)
```

Outside both blocks, the executor canceled the pilot jobs
and the server shut down.
But the optimizer kept every point it evaluated.
So this is an ordinary local value.
`opt.best_output(RUN_NAME)` is the whole mapping the objective returned there,
`distance_from_origin` included.

The program then reports which of the four known minima it landed nearest.
Which one that is depends on the seed.
All four are equally good.
The search settles on whichever its batches reached first.

We drove a Gaussian process across a pool of 80 workers.
We landed on one of the four minima of a function
we never told the optimizer anything about.
The same program, with a different objective and a different space,
is a calibration of a real model.

## Next steps

- [How to resume a search](../how-to-guides/resume-a-search.md)
    carries a search on across a time limit,
    which is what the two saved files are for.
- [`OptimizeSpaceBotorch`](../reference/optimize-space.md) is the full API:
    several spaces searched at once,
    how the search decides to stop, and the acquisition settings.
- [Search spaces](../reference/search-space.md) covers integer,
    categorical and log-scaled parameters.
- [About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md)
    says why the search has this shape,
    and when it is worth its overhead.
