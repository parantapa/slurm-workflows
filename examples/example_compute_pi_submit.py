"""Compute pi on the `bii` partition of Rivanna, one task at a time.

This program does the same computation as `example_compute_pi.py`,
with `submit` and `wait` instead of `map_reduce`.
`docs/tutorials/computing-pi-with-submit.md` explains this program step by step.
"""

from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

SETUP_SCRIPT = ""

NUM_NODES = 2
# A node of the `bii` partition has 40 cores.
NTASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={NTASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]


def do_step_pi(start: int, stop: int, step: int, stepsize: float) -> float:
    """Sum 4 / (1 + x*x) at every `step`-th midpoint, unscaled by `stepsize`."""
    x, s = 0.0, 0.0
    for i in range(start, stop, step):
        x = (i + 0.5) * stepsize
        s += 4.0 / (1.0 + x * x)
    return s


def main() -> None:
    # `ib0` is the InfiniBand interface, which the compute nodes can reach.
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor("compute-pi-submit", address) as executor:
            executor.define_job_group(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_jobs("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

            # The over-decomposition factor splits the work
            # into more tasks than there are workers.
            # That balances the load when some tasks take longer than others.
            # Ten is a good rule of thumb.
            over_decomp_factor = 10
            num_pi_tasks = NUM_NODES * NTASKS_PER_NODE * over_decomp_factor

            tasks = []
            for i in range(num_pi_tasks):
                task = executor.submit(
                    "bii",
                    do_step_pi,
                    start=i,
                    stop=num_steps,
                    step=num_pi_tasks,
                    stepsize=stepsize,
                )
                executor.set_task_name(task, f"task-{i:04d}")
                tasks.append(task)

            executor.wait(tasks, desc="compute-pi", unit="task")

    pi = sum(task.output for task in tasks) * stepsize
    print(f"pi = {pi}")


if __name__ == "__main__":
    main()
