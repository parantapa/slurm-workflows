"""Batch Bayesian optimization on Rivanna's BII cluster.

`docs/tutorials/optimizing-himmelblau.md` walks through this program.
"""

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
