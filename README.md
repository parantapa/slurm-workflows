# slurm-workflows: HPC workflow helpers for Slurm clusters.

![Futuristic banner image.](extra/banner-image.png "Futuristic banner image.")

`slurm-workflows` lets you run Python functions on a Slurm cluster
without writing sbatch scripts by hand.
It provides an interface inspired by
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html)
that launches long-lived **pilot wokers** and dispatches tasks to them,
so that Slurm's queueing latency is paid once per worker instead of once per task.

### Features

- **Pilot-worker task execution** - pay Slurm's queue latency once per worker,
    then dispatch tasks to them.
- **Dynamic scaling** - grow or shrink a pool of workers at runtime.
- **Stateful actors** - keep expensive per-worker state
    (loaded models, DB connections) warm across many tasks.
- **Transparent serialization** - functions, arguments, and return values
    are transferred using [cloudpickle](https://github.com/cloudpipe/cloudpickle).
- **Live monitoring tool** - [`swtop`](docs/how-to-use-swtop.md),
    a terminal UI showing the tasks, workers, nodes and jobs
    for a running workflow.
- **Bayesian optimization** - a [botorch](https://botorch.org/) based optimizer
    for running optimization / calibration workflows.

## Requirements

- Python >= 3.12
- Access to a Slurm cluster (`sbatch`, `squeue`, `scancel` on `PATH`)
- A running [`ds-service`](https://github.com/parantapa/ds-service) server, v5.0.0 or later

## Installation

```sh
pip install -U slurm-workflows
```

For detailed instructions on setting up on UVA's Rivanna cluster follow
[Installation and setup on Rivanna](docs/installation-and-setup-instructions-for-rivanna.md).

## Documentation

| Document | What it covers |
| --- | --- |
| [Installation and setup on Rivanna](docs/installation-and-setup-instructions-for-rivanna.md) | Installing the `ds-service` server and `slurm-workflows` on Rivanna. |
| [Tutorial: computing pi on a cluster](docs/tutorial-computing-pi.md) | A tutorial showing main features of `slurm-workflows`, by using it to create a worker pool for computing $\pi$. |
| [Tutorial: computing pi with a Sobol' QMC sweep](docs/tutorial-computing-pi-qmc.md) | A tutorial on how to use `ExploreSpaceSobolQMC` to create a space filling design and using `slurm-workflows` to evaluate it. |
| [Tutorial: optimizing Himmelblau's function](docs/tutorial-optimize-himmelblau.md) | A tutorial on how to use `OptimizeSpaceBotorch` to create a calibration / optimization task and using `slurm-workflows` to run it. |
| [How to use `swtop`](docs/how-to-use-swtop.md) | How to use `swtop` for monitoring a workflow created using `slurm-workflows`. |
| [Concepts and usage](docs/concepts.md) | Understanding the design on `slurm-workflows`. |
| [Reference](docs/reference.md) | Reference for the major user facing classes provided by `slurm-workflows`: `SlurmPilotExecutor`, `ExploreSpaceSobolQMC`, `OptimizeSpaceBotorch`. |
| [Developer notes](docs/developer-notes.md) | Notes for anyone working on `slurm-workflows` itself. |
| [How to run the tests](docs/how-to-run-tests.md) | Organization of the unit tests and instructions for running them. |

## License

MIT - see [LICENSE](LICENSE).
