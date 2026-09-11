# slurm-workflows: HPC workflow helpers for Slurm clusters

![Futuristic banner image.](extra/banner-image.png "Futuristic banner image.")

`slurm-workflows` lets you run Python functions on a Slurm cluster
without sbatch scripts written by hand.
It provides an interface inspired by
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).
The interface launches long-lived **pilot workers**.
It then dispatches tasks to those workers.
You pay Slurm's queue latency once per worker, not once per task.

Use it in three cases:

- You have many Python tasks to run on one cluster allocation.
- A sweep or a calibration has to spread across a pool of nodes.
- Per-worker state is expensive, and you want it to stay warm between tasks.

## Features

- **Pilot workers** - pay Slurm's queue latency once per worker,
    then dispatch tasks to them.
- **Dynamic scaling** - grow or shrink a pool of workers at runtime.
- **Stateful actors** - keep expensive per-worker state
    (loaded models, database connections) warm across many tasks.
- **Transparent serialization** -
    [cloudpickle](https://github.com/cloudpipe/cloudpickle)
    serializes functions, arguments, and return values.
- **Live monitoring tool** - [`swtop`](docs/reference/swtop.md),
    a terminal UI that shows the tasks, workers, nodes and jobs
    for a running workflow.
- **Bayesian optimization** - an optimizer built on [botorch](https://botorch.org/)
    for optimization and calibration workflows.

## Requirements

- Python >= 3.12
- Access to a Slurm cluster (`sbatch`, `squeue`, `scancel` on `PATH`)
- A running [`ds-service`](https://github.com/parantapa/ds-service) server

## Installation

```sh
pip install -U slurm-workflows
```

To set up on UVA's Rivanna cluster, read
[How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md).

## Usage

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor


def square(x):
    return x * x


SETUP_SCRIPT = """
module load gcc/14.2.0
conda activate my-env
"""

with DsServiceServer(interface="ib0") as ds_service:
    ds_service.wait_until_ready()

    with SlurmPilotExecutor("my-run", ds_service.address) as executor:
        # 1. Describe a kind of worker. This submits nothing.
        executor.define_worker(
            name="cpu",
            sbatch_args=["-A my_alloc", "-p standard", "-t 01:00:00"],
            setup_script=SETUP_SCRIPT,
        )

        # 2. Launch 4 pilot jobs of that kind.
        executor.scale_workers("cpu", 4)

        # 3. Submit tasks to a named queue. Workers of that group pull from it.
        tasks = [executor.submit("cpu", square, i) for i in range(100)]

        # 4. Block until every result is in.
        executor.wait(tasks, desc="squaring")

# The executor canceled every pilot job at the end of the block.
print(sum(task.output for task in tasks))
```

The executor passes `sbatch_args` straight through to `sbatch`.
As a result, any Slurm option works.
You can submit tasks before the workers exist.
The tasks wait on the queue until a pilot job starts and takes them.

## Documentation

### Tutorials

| Document | What it covers |
| --- | --- |
| [Computing pi on a Slurm cluster](docs/tutorials/computing-pi.md) | The main features of `slurm-workflows`, by creating a worker pool to compute $\pi$. |
| [Computing pi with a Sobol' QMC sweep](docs/tutorials/computing-pi-qmc.md) | Using `ExploreSpaceSobolQMC` to create a space filling design and evaluate it. |
| [Optimizing Himmelblau's function](docs/tutorials/optimizing-himmelblau.md) | Using `OptimizeSpaceBotorch` to run a calibration / optimization task. |

### How-to guides

| Document | What it covers |
| --- | --- |
| [How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md) | Installing the package and the `ds-service` binary on Rivanna. |
| [How to run the task-queue server](docs/how-to-guides/run-the-task-queue-server.md) | Starting a `ds-service` server from the driver and binding it where workers can reach it. |
| [How to keep per-worker state with actors](docs/how-to-guides/keep-per-worker-state-with-actors.md) | Loading an expensive model or connection once per worker instead of once per task. |
| [How to watch a run with `swtop`](docs/how-to-guides/watch-a-run-with-swtop.md) | Following a live run from another shell, and keeping a record of one. |
| [How to troubleshoot a failing run](docs/how-to-guides/troubleshoot-a-failing-run.md) | Finding the right log, and what each `RuntimeError` means. |
| [How to resume a search](docs/how-to-guides/resume-a-search.md) | Carrying an optimization on across a walltime limit. |

### Reference

| Document | What it covers |
| --- | --- |
| [`SlurmPilotExecutor`](docs/reference/executor.md) | The coordinator, `Task`, `RaiseOnError`, worker group options, what a run publishes, and the logs. |
| [`ExploreSpaceSobolQMC`](docs/reference/explore-space.md) | The Sobol' sweep, the objective contract, and the results file. |
| [`OptimizeSpaceBotorch`](docs/reference/optimize-space.md) | The batch Bayesian search, its task fields, and its stopping rule. |
| [Search spaces](docs/reference/search-space.md) | `IntRange`, `FloatRange` and `CategoricalRange`. |
| [`swtop`](docs/reference/swtop.md) | The CLI, the blocks on screen, and what the host and job readings measure. |

### Explanation

| Document | What it covers |
| --- | --- |
| [About the pilot-job model](docs/explanation/about-the-pilot-job-model.md) | Why pilot workers, the three processes, and which class to reach for. |
| [About batch Bayesian optimization](docs/explanation/about-batch-bayesian-optimization.md) | Why a search has rounds, where the fit runs, and when it is worth the overhead. |
| [About what a run publishes](docs/explanation/about-what-a-run-publishes.md) | Why a run is observable from outside itself, and the limits of that. |

### For contributors

| Document | What it covers |
| --- | --- |
| [Developer notes](docs/developer-notes.md) | Notes for anyone working on `slurm-workflows` itself. |
| [How to run the tests](docs/how-to-run-tests.md) | Organization of the unit tests and instructions for running them. |

## License

MIT - see [LICENSE](LICENSE).
