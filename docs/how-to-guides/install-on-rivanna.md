# How to install slurm-workflows on Rivanna

[<- back to the main README](../../README.md)

Nothing in slurm-workflows runs until you have two things on Rivanna.
You need a Python 3.12 environment with `slurm-workflows` in it,
and the `ds-service` binary on your `PATH`.
These steps give you both.

## 1. Load miniforge

```sh
module load miniforge/26.3.2
```

## 2. Create the conda environment

```sh
conda create -y -n slurm-workflows python=3.12
conda activate slurm-workflows
```

If `conda activate` fails with a message about `conda init`,
source conda's shell hook.
Then activate the environment again:

```sh
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate slurm-workflows
```

## 3. Install slurm-workflows

```sh
pip install -U slurm-workflows
```

To run a Sobol' exploration or a Bayesian search,
also install the separate
[`slurm-workflows-optimize`](https://github.com/parantapa/slurm-workflows-optimize)
package.
Its own README gives the install command.

## 4. Install the `ds-service` binary

`ds-service` is a static binary.
It has its own releases, separate from this package.
Download the latest `ds-service` release onto your `PATH`:

```sh
mkdir -p ~/bin
curl -L -o ~/bin/ds-service \
    https://github.com/parantapa/ds-service/releases/latest/download/ds-service
chmod +x ~/bin/ds-service
```

If `~/bin` is not already on your `PATH`, add it:

```sh
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
export PATH="$HOME/bin:$PATH"
```

## 5. Check the installation

```sh
ds-service --help
swtop --help
```

Both must print usage text.

* `ds-service: command not found` means the `PATH` change in step 4
    is not active in this shell.
    Run the `export PATH` line from step 4 again,
    or open a new shell.
* `swtop: command not found` means the `slurm-workflows` environment
    is not active.
    Run `conda activate slurm-workflows` again.

## Related

- [Computing pi on a Slurm cluster](../tutorials/computing-pi.md),
    the first tutorial, which needs exactly this setup
- [How to run the `ds-service` server](run-the-ds-service-server.md),
    for starting `ds-service` from your driver
- [The pilot-job model](../explanation/pilot-job-model.md),
    for what `ds-service` does in a run
