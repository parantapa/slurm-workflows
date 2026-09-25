"""Compute pi with a Sobol' QMC exploration on the `bii` partition of Rivanna.

`docs/tutorials/computing-pi-qmc.md` walks through this program.
"""

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
