# slurm-workflows: HPC workflow helpers for Slurm clusters.

![Futuristic banner image.](extra/banner-image.png "Futuristic banner image.")

`slurm-workflows` lets you run Python functions on a Slurm cluster
without writing sbatch scripts by hand.
It provides an interface inspired by
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html)
that launches long-lived **pilot jobs** and dispatches tasks to them,
so Slurm's queueing latency is paid once per worker instead of once per task.

### Features

- **Pilot-job task execution** - pay the queue wait once,
    then dispatch tasks at queue-latency speed.
- **Dynamic scaling** - grow or shrink a pool of workers at runtime.
- **Stateful actors** - keep expensive per-worker state
    (loaded models, DB connections) warm across many tasks.
- **Transparent serialization** - functions, arguments, and return values
    are transferred using [cloudpickle](https://github.com/cloudpipe/cloudpickle).
- **Live queue view** - [`swtop`](docs/how-to-use-swtop.md), a terminal UI showing
    the tasks, workers, nodes and jobs of a running search.
- **Bayesian optimization** - a [botorch](https://botorch.org/) based optimizer
    for running optimization / calibration workflows.

## Requirements

- Python >= 3.12
- Access to a Slurm cluster (`sbatch`, `squeue`, `scancel` on `PATH`)
- A running [`ds-service`](https://github.com/parantapa/ds-service) server, v5.0.0 or later.

## Installation

```sh
pip install -U slurm-workflows
```

On Rivanna, follow
[Installation and setup on Rivanna](docs/installation-and-setup-instructions-for-rivanna.md)
instead: it covers the modules, the conda environment,
and the `ds-service` binary.

## Documentation

| Document | What it covers |
| --- | --- |
| [Installation and setup on Rivanna](docs/installation-and-setup-instructions-for-rivanna.md) | Setting up the conda environment, installing `slurm-workflows` with the botorch extra, and getting the `ds-service` binary onto Rivanna. |
| [Tutorial: computing pi on a cluster](docs/tutorial-computing-pi.md) | A walkthrough of `examples/example_compute_pi.py`: the queue server, a worker group, a pool of pilot workers, and getting the results back. |
| [Tutorial: computing pi with a Sobol' QMC sweep](docs/tutorial-computing-pi-qmc.md) | The same calculation with `ExploreSpaceSobolQMC`: a design over a search space, evaluated across the pool, with the submit-and-wait loop handled for you. |
| [Tutorial: optimizing Himmelblau's function](docs/tutorial-optimize-himmelblau.md) | A batch Bayesian search on the cluster: a Sobol' sweep, then rounds of fit, propose and evaluate across two worker groups. |
| [How to use `swtop`](docs/how-to-use-swtop.md) | The live view of a running queue: the CLI, what each block shows, what the host and job readings measure, and reading it as plain text. |
| [How to run the tests](docs/how-to-run-tests.md) | Running the suite, what is mocked and what is real, and notes for changing the tests. |
| [Concepts and usage](docs/concepts.md) | Concepts, quick start, `SlurmPilotExecutor`, `Task`, `RaiseOnError`, stateful actors, the worker environment, and troubleshooting. |
| [Reference](docs/reference.md) | The three classes a program is written against - `SlurmPilotExecutor`, `ExploreSpaceSobolQMC`, `OptimizeSpaceBotorch` - what to call on each, the objective contract, search spaces, and how a botorch search rounds and stops. |
| [Developer notes](docs/developer-notes.md) | Working on `slurm-workflows` itself: where the code lives, the invariants that are quiet when broken, and the conventions a change is checked against. |

## License

MIT - see [LICENSE](LICENSE).
