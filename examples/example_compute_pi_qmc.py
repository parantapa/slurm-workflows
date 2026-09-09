"""Compute pi with a Sobol' QMC sweep on Rivanna's BII cluster.

Walked through in `docs/tutorials/computing-pi-qmc.md`.
"""

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
