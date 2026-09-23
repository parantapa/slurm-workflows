# slurm-workflows: HPC workflow helpers for Slurm clusters

![Futuristic banner image.](extra/banner-image.png "Futuristic banner image.")

`slurm-workflows` lets you run Python functions on a Slurm cluster
without sbatch scripts written by hand.
It provides an interface
inspired by [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).
The interface launches long-lived **workers** inside pilot jobs.
It then dispatches tasks to those workers.
You pay Slurm's scheduling latency once per pilot job, not once per task.

Use it in three cases:

- You have many Python tasks to run on one cluster allocation.
- An exploration or a search has to spread across a pool of workers.
- Per-worker state is expensive, and you want it to stay warm between tasks.

## Installation

A run needs:

- Python >= 3.12
- Access to a Slurm cluster (`sbatch`, `squeue`, `scancel` on `PATH`)
- The [`ds-service`](https://github.com/parantapa/ds-service) binary on `PATH`

```sh
pip install -U slurm-workflows
```

To set up on UVA's Rivanna cluster, read
[How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md).

## Usage

Replace the Slurm account (`-A`), the partition (`-p`)
and the setup script with the ones for your cluster.
The driver must run on a node with the `ib0` interface.
For another interface, read
[How to run the `ds-service` server](docs/how-to-guides/run-the-ds-service-server.md).

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
        # 1. Describe a job group. This submits nothing.
        executor.define_job_group(
            name="cpu",
            sbatch_args=["-A my_alloc", "-p standard", "-t 01:00:00"],
            setup_script=SETUP_SCRIPT,
        )

        # 2. Launch 4 pilot jobs of that job group.
        executor.scale_jobs("cpu", 4)

        # 3. Submit tasks to a named queue. That job group's workers claim them.
        tasks = [executor.submit("cpu", square, i) for i in range(100)]

        # 4. Block until every result is in.
        executor.wait(tasks, desc="squaring")

# The executor canceled every pilot job at the end of the block.
print(sum(task.output for task in tasks))
```

```text
work directory: '/home/<user>/.cache/slurm-workflows/my-run/<timestamp>'
328350
```

You can submit tasks before the workers exist.
They wait on the queue until a worker starts and claims them.

## Documentation

| Document | What it covers |
| --- | --- |
| [Computing pi on a Slurm cluster](docs/tutorials/computing-pi.md) | The main features of `slurm-workflows`, by creating a pool of workers to compute $\pi$. |
| [Computing pi with a Sobol' QMC exploration](docs/tutorials/computing-pi-qmc.md) | Using `ExploreSpaceSobolQMC` to create a space filling design and evaluate it. |
| [Optimizing Himmelblau's function](docs/tutorials/optimizing-himmelblau.md) | Using `OptimizeSpaceBotorch` to run a batch Bayesian search. |
| [How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md) | Installing the package and the `ds-service` binary on Rivanna. |
| [How to run the `ds-service` server](docs/how-to-guides/run-the-ds-service-server.md) | Starting a `ds-service` server from the driver and binding it where workers can reach it. |
| [How to keep per-worker state with actors](docs/how-to-guides/keep-per-worker-state-with-actors.md) | Loading an expensive model or connection once per worker instead of once per task. |
| [How to fold results across workers](docs/how-to-guides/fold-results-across-workers.md) | Using `mapreduce` to run one function over a whole collection and bring back a single value. |
| [How to watch a run with `swtop`](docs/how-to-guides/watch-a-run-with-swtop.md) | Following a live run from another shell, and keeping a record of one. |
| [How to troubleshoot a failing run](docs/how-to-guides/troubleshoot-a-failing-run.md) | Finding the right log, and what each `RuntimeError` means. |
| [How to resume a search](docs/how-to-guides/resume-a-search.md) | Carrying a search on across a Slurm time limit. |
| [`SlurmPilotExecutor`](docs/reference/executor.md) | The executor, `Task`, `RaiseOnError`, and the job group options. |
| [`mapreduce`](docs/reference/mapreduce.md) | Mapping an iterable across the pool, the fold contract, and what the call creates on the server. |
| [What a run publishes](docs/reference/what-a-run-publishes.md) | The environment a task sees, the keys and series a run writes, the worker entry point, and the logs. |
| [`ExploreSpaceSobolQMC`](docs/reference/explore-space.md) | The Sobol' exploration, its study fields, and the results file. |
| [`OptimizeSpaceBotorch`](docs/reference/optimize-space.md) | The batch Bayesian search, its study fields, and its stopping rule. |
| [Search spaces](docs/reference/search-space.md) | `IntRange`, `FloatRange` and `CategoricalRange`, the objective contract, and what a failed evaluation does to a run. |
| [`swtop`](docs/reference/swtop.md) | The CLI, the blocks on screen, and what the host and job readings measure. |
| [The pilot-job model](docs/explanation/pilot-job-model.md) | Why pilot jobs, the three processes, where the driver runs, and which class to reach for. |
| [Batch Bayesian optimization](docs/explanation/batch-bayesian-optimization.md) | Why a search has rounds, where the fit runs, and when it is worth the overhead. |
| [The trail a run leaves](docs/explanation/the-trail-a-run-leaves.md) | Why a run is observable from outside itself, and the limits of that. |

## For contributors

| Document | What it covers |
| --- | --- |
| [Developer notes](docs/developer-notes.md) | The layout of the code, where each kind of documentation goes, and the conventions a change is held to. |
| [Terminology](docs/terminology.md) | The word this project uses for each concept, in prose and in identifiers, and where each word comes from. |
| [How to run the tests](docs/how-to-run-tests.md) | Running the suite, what it mocks, and what it runs for real. |

## License

MIT. See [LICENSE](LICENSE).
