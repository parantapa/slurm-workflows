# How to install slurm-workflows on Rivanna

[<- back to the main README](../../README.md)

Nothing here runs until you have two things on Rivanna.
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

The Python version must be >= 3.12.
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

If you will run a Bayesian search,
install the `botorch` extra instead:

```sh
pip install -U "slurm-workflows[botorch]"
```

That extra brings in botorch and torch,
which the Bayesian optimizer needs.
Leave it out for anything else,
and you leave torch out with it.

## 4. Install the ds-service binary

`ds-service` is a static binary.
The project releases it separately from this package.
Always download the latest release,
and put it somewhere on your `PATH`:

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
    for what `ds-service` is doing in a run
