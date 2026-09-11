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
pip install -U "slurm-workflows[botorch]"
```

The `botorch` extra installs botorch and torch.
The Bayesian optimizer needs both.

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
* `swtop: command not found` means the `slurm-workflows` environment
    is not active.
    Run `conda activate slurm-workflows` again.

## Next steps

Read [Computing pi on a Slurm cluster](../tutorials/computing-pi.md)
