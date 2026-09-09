# How to install slurm-workflows on Rivanna

[<- back to the main README](../../README.md)

These steps set up `slurm-workflows` for use on the Rivanna cluster at UVA.

## 1. Load miniforge

```sh
module load miniforge/26.3.2
```

## 2. Create the conda environment

```sh
conda create -y -n slurm-workflows python=3.12
conda activate slurm-workflows
```

Python version used must be >= 3.12.
If `conda activate` fails with a message about `conda init`,
source conda's shell hook and try again:

```sh
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate slurm-workflows
```

## 3. Install slurm-workflows

```sh
pip install -U "slurm-workflows[botorch]"
```

The `botorch` extra pulls in botorch and torch,
which are needed by the Bayesian optimizer.

## 4. Install the ds-service binary

`ds-service` is a static binary, released separately from this package.
Put it somewhere on your `PATH`:

```sh
mkdir -p ~/bin
curl -L -o ~/bin/ds-service \
    https://github.com/parantapa/ds-service/releases/download/v5.1.0/ds-service
chmod +x ~/bin/ds-service
```

If `~/bin` is not already on your `PATH`, add it:

```sh
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
export PATH="$HOME/bin:$PATH"
```

This version of `slurm-workflows` requires `ds-service` >= v5.1.0.

## 5. Check the installation

```sh
ds-service --help
swtop --help
```

Both must print usage text.

* `ds-service: command not found` means step 4's `PATH` change
    has not taken effect in this shell.
* `swtop: command not found` means the `slurm-workflows` environment
    is not the active one; run `conda activate slurm-workflows` again.

## Next steps

Check out [Tutorial: Computing pi on a Slurm cluster](../tutorials/computing-pi.md)
