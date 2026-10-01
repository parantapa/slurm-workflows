# slurm-workflows: HPC workflow helpers for Slurm clusters

`slurm-workflows` runs Python functions on a Slurm cluster
from a Python script.
It provides an interface
inspired by [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).
The interface launches long-lived **workers** inside pilot jobs.
It then dispatches tasks to those workers.
You pay Slurm's scheduling latency once per pilot job.
Every later task runs on a worker that started earlier.

![Futuristic banner image.](extra/banner-image.png "Futuristic banner image.")

Use it in three cases:

- You have one Python function to run over many inputs on one cluster allocation.
- You have many Python tasks to run, some of which wait on others.
- Per-worker state is expensive to build, and you want to keep it between tasks.

For a search over a parameter space, use
[`slurm-workflows-optimize`](https://github.com/parantapa/slurm-workflows-optimize),
which builds on this package.

## Installation

A run needs Slurm's `sbatch`, `squeue` and `scancel`,
and the [`ds-service`](https://github.com/parantapa/ds-service) binary, on `PATH`.

```sh
pip install -U slurm-workflows
```

To install on UVA's Rivanna cluster, read
[How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md).

## Usage

Replace the Slurm account (`-A`), the partition (`-p`)
and the setup script with the ones for your cluster.
The driver, the Python script that uses the executor,
must run on a node with the `ib0` interface.
For another interface, read
[How to run the `ds-service` server](docs/how-to-guides/run-the-ds-service-server.md).

```python
from ds_service_client import DsServiceServer
from slurm_workflows import SlurmPilotExecutor


def square(x):
    return x * x


SETUP_SCRIPT = """
module load miniforge/26.3.2
conda activate slurm-workflows
"""

# The network interface the workers can reach.
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

        # 3. Map `square` over the inputs on the "cpu" job group's workers.
        #    The call blocks until every value is back, in input order.
        squares = executor.map("cpu", square, range(100))

# The executor canceled every pilot job at the end of the block.
print(sum(squares))
```

```text
work directory: '/home/<user>/.cache/slurm-workflows/my-run/<timestamp>'
328350
```

`map` blocks, and hands its inputs to the workers as they start.
For one value instead of a list, `map_reduce` folds the values on the workers.
For a handle per task, `submit` enqueues one task and returns at once,
even before the workers exist.
To choose between them, read [The pilot-job model](docs/explanation/pilot-job-model.md).

## Documentation

| Type | Document | What it covers |
| --- | --- | --- |
| Tutorial | [Computing pi on a Slurm cluster](docs/tutorials/computing-pi.md) | The main features of `slurm-workflows`, by using `map_reduce` on a pool of workers to compute $\pi$. |
| Tutorial | [Computing pi with `submit` and `wait`](docs/tutorials/computing-pi-with-submit.md) | The advanced interface: one task at a time, with task names, priorities, parent tasks and a failure policy. |
| How-to guide | [How to install slurm-workflows on Rivanna](docs/how-to-guides/install-on-rivanna.md) | Installing the package and the `ds-service` binary on Rivanna. |
| How-to guide | [How to run the `ds-service` server](docs/how-to-guides/run-the-ds-service-server.md) | Starting a `ds-service` server from the driver and binding it where workers can reach it. |
| How-to guide | [How to keep per-worker state with actors](docs/how-to-guides/keep-per-worker-state-with-actors.md) | Loading an expensive model or connection once per worker instead of once per task. |
| How-to guide | [How to watch a run with `swtop`](docs/how-to-guides/watch-a-run-with-swtop.md) | Following a live run from another shell, and keeping a record of one. |
| How-to guide | [How to embed `swtop` in a Textual app](docs/how-to-guides/embed-swtop-in-a-textual-app.md) | Putting the `swtop` tabs, summary line, progress bar and error line in your own Textual app. |
| How-to guide | [How to troubleshoot a failing run](docs/how-to-guides/troubleshoot-a-failing-run.md) | Finding the right log, and what each `RuntimeError` means. |
| How-to guide | [How to update worker code without resubmitting](docs/how-to-guides/update-worker-code-without-resubmitting.md) | Restarting the workers in running pilot jobs, so they run new code without a second wait in Slurm's queue. |
| Reference | [`map`](docs/reference/map.md) | Mapping an iterable across the pool and getting every value back in order. |
| Reference | [`map_reduce`](docs/reference/map-reduce.md) | Mapping an iterable across the pool, the fold contract, and what the call creates on the server. |
| Reference | [`submit`, `wait` and `as_completed`](docs/reference/submit-and-wait.md) | The advanced interface: `submit` options, `Task`, `RaiseOnError`, and the errors that end a wait. |
| Reference | [`SlurmPilotExecutor`](docs/reference/executor.md) | The executor, the job group options, restarting workers, and the worker entry point. |
| Reference | [What a run publishes](docs/reference/what-a-run-publishes.md) | The keys and time series a run writes, and the logs. |
| Reference | [`swtop`](docs/reference/swtop.md) | The CLI, the keys, the blocks on screen, and what the host and job readings measure. |
| Reference | [`slurm_workflows.swtop_widgets`](docs/reference/swtop-widgets.md) | The `swtop` widgets and the poller an app can embed. |
| Explanation | [The pilot-job model](docs/explanation/pilot-job-model.md) | Why pilot jobs, the three processes, where the driver runs, and the two interfaces. |
| Explanation | [The monitoring state a run publishes](docs/explanation/monitoring-state-a-run-publishes.md) | Why a run is observable from outside itself, and the limits of that. |

## For contributors

| Document | What it covers |
| --- | --- |
| [Developer notes](docs/developer-notes.md) | The layout of the code, where each kind of documentation goes, and the conventions a change must follow. |
| [Terminology](docs/terminology.md) | The word this project uses for each concept, in prose and in identifiers, and where each word comes from. |
| [How to run the tests](docs/how-to-run-tests.md) | Running the suite, what it mocks, and what it runs for real. |

## License

MIT. See [LICENSE](LICENSE).
