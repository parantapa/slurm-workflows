"""Submit, list and cancel Slurm jobs."""

import os
import re
import subprocess
from pathlib import Path
from dataclasses import dataclass
from functools import cache

from .templates import render_template

# Seconds any one Slurm command can run before the timeout stops it.
COMMAND_TIMEOUT: int = 120
SBATCH_OUTPUT_REGEX = re.compile(r"Submitted batch job (?P<id>\S*)")

SBATCH_EXE = "sbatch"
SQUEUE_EXE = "squeue"
SCANCEL_EXE = "scancel"


@cache
def get_clean_environ() -> dict[str, str]:
    """The environment, less every Slurm-set variable.

    Drops `SLURM_`, `SLURMD_`, `PMI_` and `SRUN_`.
    A job submitted from inside an allocation
    takes none of that allocation's settings.

    The result is built once per process and shared between callers.
    A later change to `os.environ` does not show in it,
    and a caller must not modify it.
    """
    # `sbatch` reads `SLURM_*` variables as defaults,
    # so a driver inside an allocation would give every pilot job
    # its own node count and Slurm task count.
    sanitized_env: dict[str, str] = {}
    for k, v in os.environ.items():
        if (
            k.startswith("PMI_")
            or k.startswith("SLURM_")
            or k.startswith("SLURMD_")
            or k.startswith("SRUN_")
        ):
            continue

        sanitized_env[k] = v

    return sanitized_env


def get_running_jobids() -> set[int]:
    """The ids of this user's Slurm jobs, pending or running.

    Raises `subprocess.CalledProcessError` if `squeue` fails,
    and `subprocess.TimeoutExpired` if it does not answer in time.
    """
    cmd = [SQUEUE_EXE, "--all", "--me", "--noheader", "--format", "%A"]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        check=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
    )
    job_ids = proc.stdout.strip().split()
    job_ids = set(int(j) for j in job_ids)
    return job_ids


def cancel_jobs(
    job_ids: list[int],
    term: bool = False,
    batch: bool = False,
    full: bool = False,
) -> None:
    """Cancel the given jobs.

    An empty list is a no-op.
    `term`, `batch` and `full` add `scancel`'s `--signal=TERM`,
    `--batch` and `--full` respectively.
    Raises `subprocess.CalledProcessError` if `scancel` fails,
    and `subprocess.TimeoutExpired` if it does not answer in time.
    """
    if not job_ids:
        return

    cmd = [SCANCEL_EXE]
    if term:
        cmd.append("--signal=TERM")
    if batch:
        cmd.append("--batch")
    if full:
        cmd.append("--full")
    cmd.extend([str(id) for id in job_ids])

    subprocess.run(
        cmd, capture_output=True, check=True, text=True, timeout=COMMAND_TIMEOUT
    )


@dataclass
class SlurmJob:
    """A submitted Slurm job."""

    name: str
    sbatch_args: list[str]
    script: str

    job_id: int
    output_file: Path


def submit_sbatch_job(
    name: str,
    sbatch_args: list[str],
    script: str,
    work_dir: Path,
) -> SlurmJob:
    """Submit one job, and return it with its id and output file resolved.

    Writes `<name>.sbatch` into `work_dir` and makes it executable.
    `sbatch` runs in the environment `get_clean_environ()` returns.
    Raises `RuntimeError` if `sbatch` succeeds but its output holds no job id,
    `ValueError` if the job id it prints is not a number,
    `subprocess.CalledProcessError` if `sbatch` fails,
    and `subprocess.TimeoutExpired` if it does not answer in time.
    """
    output_file = str(work_dir / f"{name}-%j.out")

    script_path = work_dir / f"{name}.sbatch"
    script_text = render_template(
        "slurm_utils:script_template",
        name=name,
        sbatch_args=sbatch_args,
        script=script,
        output_file=output_file,
    )
    script_path.write_text(script_text)
    os.chmod(script_path, mode=0o755)

    proc = subprocess.run(
        [SBATCH_EXE, str(script_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
        env=get_clean_environ(),
    )

    # A site can print a banner, so this searches the output,
    # not only the start.
    match = SBATCH_OUTPUT_REGEX.search(proc.stdout)
    if match is None:
        raise RuntimeError("Failed to parse sbatch output", proc, match)
    job_id = match.group("id")
    job_id = int(job_id)

    # Slurm expands %j itself.
    # Do the same here, so the caller has a real path.
    output_file = Path(output_file.replace("%j", str(job_id)))

    return SlurmJob(
        name=name,
        sbatch_args=sbatch_args,
        script=script,
        job_id=job_id,
        output_file=output_file,
    )
