"""Compute pi on Rivanna's BII cluster.

Walked through in `docs/tutorial-computing-pi.md`.
"""

from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor

SETUP_SCRIPT = ""

NUM_NODES = 2
TASKS_PER_NODE = 40

SBATCH_ARGS = [
    "--account=bii_nssac",
    f"--partition=bii --nodes={NUM_NODES}",
    f"--ntasks-per-node={TASKS_PER_NODE} --cpus-per-task=1 --mem=0",
    "--time=1:00:00",
]


def do_step_pi(start, stop, step, stepsize):
    """Sum every `step`-th midpoint slice, beginning at `start`."""
    x, s = 0.0, 0.0
    for i in range(start, stop, step):
        x = (i + 0.5) * stepsize
        s += 4.0 / (1.0 + x * x)
    return s


def main():
    with DsServiceServer(interface="ib0") as ds_service:
        ds_service.wait_until_ready()
        address = ds_service.address

        with SlurmPilotExecutor("compute-pi", address) as executor:
            executor.define_worker(
                name="bii",
                sbatch_args=SBATCH_ARGS,
                setup_script=SETUP_SCRIPT,
            )
            executor.scale_workers("bii", 1)

            num_steps = 1_000_000_000
            stepsize = 1.0 / num_steps

            over_decomp_factor = 10
            num_tasks = NUM_NODES * TASKS_PER_NODE * over_decomp_factor

            tasks = []
            for i in range(num_tasks):
                task = executor.submit(
                    "bii",
                    do_step_pi,
                    start=i,
                    stop=num_steps,
                    step=num_tasks,
                    stepsize=stepsize,
                )
                executor.set_task_name(task, f"task-{i:04d}")
                tasks.append(task)

            executor.wait(tasks, desc="compute-pi", unit="slice")

    pi = sum(task.output for task in tasks) * stepsize
    print(f"pi = {pi}")


if __name__ == "__main__":
    main()
