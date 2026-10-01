"""Tests for the Slurm command wrappers, with mocked sbatch, squeue and scancel."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Generator
from pathlib import Path

import pytest

from slurm_workflows import slurm_utils
from slurm_workflows.testing import FakeSlurm
from slurm_workflows.slurm_utils import (
    COMMAND_TIMEOUT,
    cancel_jobs,
    get_clean_environ,
    get_running_jobids,
    submit_sbatch_job,
)


@pytest.fixture
def clean_environ_cache() -> Generator[None]:
    """Clear the `get_clean_environ` cache before and after the test."""
    get_clean_environ.cache_clear()
    try:
        yield
    finally:
        get_clean_environ.cache_clear()


# `get_clean_environ` is cached.
# The fixture clears the cache around each test,
# so no test reads the variables of another,
# even when an assertion fails.
@pytest.mark.usefixtures("clean_environ_cache")
class TestGetCleanEnviron:
    def test_strips_slurm_variables(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("SLURM_JOB_ID", "1")
        monkeypatch.setenv("SLURMD_NODENAME", "node1")
        monkeypatch.setenv("PMI_RANK", "0")
        monkeypatch.setenv("SRUN_DEBUG", "3")
        monkeypatch.setenv("PATH", "/usr/bin")
        monkeypatch.setenv("MY_VAR", "keep-me")

        env = get_clean_environ()

        assert "SLURM_JOB_ID" not in env
        assert "SLURMD_NODENAME" not in env
        assert "PMI_RANK" not in env
        assert "SRUN_DEBUG" not in env
        assert env["PATH"] == "/usr/bin"
        assert env["MY_VAR"] == "keep-me"

    def test_keeps_variables_merely_containing_slurm(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("MY_SLURM_HELPER", "keep-me")

        assert get_clean_environ()["MY_SLURM_HELPER"] == "keep-me"

    def test_caches_the_first_result_until_cache_clear(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("MY_VAR", "first")
        monkeypatch.setenv("SLURM_JOB_ID", "1")
        first = get_clean_environ()

        monkeypatch.setenv("MY_VAR", "second")
        monkeypatch.setenv("LATE_VAR", "late")

        cached = get_clean_environ()
        assert cached is first
        assert cached["MY_VAR"] == "first"
        assert "LATE_VAR" not in cached

        get_clean_environ.cache_clear()
        fresh = get_clean_environ()
        assert fresh["MY_VAR"] == "second"
        assert fresh["LATE_VAR"] == "late"
        assert "SLURM_JOB_ID" not in fresh


class TestSubmitSbatchJob:
    def test_returns_parsed_job(self, fake_slurm, tmp_path: Path):
        job = submit_sbatch_job(
            name="myjob",
            sbatch_args=["-A alloc", "-p standard"],
            script="echo hello",
            work_dir=tmp_path,
        )

        # FakeSlurm numbers its jobs from 1000.
        assert job.job_id == 1000
        assert job.name == "myjob"
        assert job.sbatch_args == ["-A alloc", "-p standard"]

    def test_writes_executable_script_with_directives(self, fake_slurm, tmp_path: Path):
        submit_sbatch_job(
            name="myjob",
            sbatch_args=["-A alloc", "-t 01:00:00"],
            script="echo hello",
            work_dir=tmp_path,
        )

        script_path = tmp_path / "myjob.sbatch"
        assert script_path.exists()
        assert script_path.stat().st_mode & 0o111, "script should be executable"

        text = script_path.read_text()
        assert text.startswith("#!/bin/bash")
        assert '#SBATCH --job-name "myjob"' in text
        assert "#SBATCH -A alloc" in text
        assert "#SBATCH -t 01:00:00" in text
        assert "echo hello" in text

    def test_resolves_job_id_in_output_file(self, fake_slurm, tmp_path: Path):
        job = submit_sbatch_job(
            name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
        )

        assert "%j" not in str(job.output_file)
        assert job.output_file == tmp_path / f"myjob-{job.job_id}.out"

    def test_submits_with_scrubbed_environment(
        self, fake_slurm, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """A submission from inside a Slurm job must not leak SLURM_* through."""
        monkeypatch.setenv("SLURM_JOB_ID", "999")
        monkeypatch.setenv("KEEP_ME", "yes")
        slurm_utils.get_clean_environ.cache_clear()

        submit_sbatch_job(
            name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
        )

        env = fake_slurm.submissions[0].env
        assert "SLURM_JOB_ID" not in env
        assert env["KEEP_ME"] == "yes"

    def test_finds_the_job_id_after_a_banner(self, fake_slurm, tmp_path: Path):
        """Sites put warnings and banners on sbatch's stdout."""
        fake_slurm.sbatch_stdout_override = (
            "sbatch: WARNING: your account is nearly out of hours\n"
            "Submitted batch job 4242\n"
        )

        job = submit_sbatch_job(
            name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
        )

        assert job.job_id == 4242

    def test_raises_on_unparsable_sbatch_output(self, fake_slurm, tmp_path: Path):
        fake_slurm.sbatch_stdout_override = "something unexpected\n"

        with pytest.raises(RuntimeError, match="Failed to parse sbatch output"):
            submit_sbatch_job(
                name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
            )

    @pytest.mark.parametrize(
        "stdout",
        ["Submitted batch job\n", "Submitted batch job \n"],
        ids=["no-id", "blank-id"],
    )
    def test_a_reply_with_no_job_id_is_unparsable(
        self, fake_slurm, tmp_path: Path, stdout: str
    ):
        fake_slurm.sbatch_stdout_override = stdout

        with pytest.raises(RuntimeError, match="Failed to parse sbatch output"):
            submit_sbatch_job(
                name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
            )

    def test_raises_on_a_non_numeric_job_id(self, fake_slurm, tmp_path: Path):
        fake_slurm.sbatch_stdout_override = "Submitted batch job abc\n"

        with pytest.raises(ValueError):
            submit_sbatch_job(
                name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
            )

    def test_runs_sbatch_on_the_script_it_wrote(self, fake_slurm, tmp_path: Path):
        submit_sbatch_job(
            name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
        )

        [(cmd, _)] = fake_slurm.calls
        assert cmd == ["sbatch", str(tmp_path / "myjob.sbatch")]

    def test_propagates_sbatch_failure(self, fake_slurm, tmp_path: Path):
        fake_slurm.fail_command("sbatch", returncode=2, stderr="invalid account")

        with pytest.raises(subprocess.CalledProcessError):
            submit_sbatch_job(
                name="myjob", sbatch_args=[], script="true", work_dir=tmp_path
            )


class TestGetRunningJobids:
    def test_asks_squeue_for_own_job_ids_only(self, fake_slurm):
        get_running_jobids()

        [(cmd, _)] = fake_slurm.calls
        assert cmd[0] == "squeue"
        assert "--me" in cmd
        assert "--noheader" in cmd
        # `%A` is the job id, one per line.
        assert cmd[cmd.index("--format") + 1] == "%A"

    def test_parses_job_ids(self, fake_slurm):
        fake_slurm.running_job_ids = [11, 22, 33]

        assert get_running_jobids() == {11, 22, 33}

    def test_empty_when_nothing_queued(self, fake_slurm):
        assert get_running_jobids() == set()


class TestCancelJobs:
    def test_cancels_given_ids(self, fake_slurm):
        fake_slurm.running_job_ids = [11, 22]

        cancel_jobs([11, 22])

        assert fake_slurm.cancelled_job_ids == [11, 22]
        assert fake_slurm.running_job_ids == []

    def test_empty_list_issues_no_command(self, fake_slurm):
        cancel_jobs([])

        # A bare `scancel` would raise in the fake, as it fails in Slurm,
        # but the empty list must not reach `run` at all.
        assert fake_slurm.calls == []
        assert fake_slurm.cancelled_job_ids == []

    def test_flags_are_passed_through(self, fake_slurm):
        cancel_jobs([7], term=True, batch=True, full=True)

        [(cmd, _)] = fake_slurm.calls
        assert cmd[0] == "scancel"
        assert "--signal=TERM" in cmd
        assert "--batch" in cmd
        assert "--full" in cmd
        assert cmd[-1] == "7"


# Each wrapper with the arguments that make it issue exactly one command.
def _submit(tmp_path: Path) -> None:
    """Submit a trivial job into `tmp_path`."""
    submit_sbatch_job(name="myjob", sbatch_args=[], script="true", work_dir=tmp_path)


def _list(tmp_path: Path) -> None:
    """List the running jobs."""
    get_running_jobids()


def _cancel(tmp_path: Path) -> None:
    """Cancel one job."""
    cancel_jobs([7])


EVERY_COMMAND: list[tuple[str, Callable[[Path], None]]] = [
    ("sbatch", _submit),
    ("squeue", _list),
    ("scancel", _cancel),
]


@pytest.mark.parametrize(
    "exe, call", EVERY_COMMAND, ids=[exe for exe, _ in EVERY_COMMAND]
)
class TestEveryCommand:
    def test_runs_with_timeout_and_check(
        self,
        fake_slurm: FakeSlurm,
        tmp_path: Path,
        exe: str,
        call: Callable[[Path], None],
    ):
        call(tmp_path)

        [(cmd, kwargs)] = fake_slurm.calls
        assert cmd[0] == exe
        assert kwargs["timeout"] == COMMAND_TIMEOUT
        assert kwargs["check"] is True

    def test_propagates_a_timeout(
        self,
        fake_slurm: FakeSlurm,
        tmp_path: Path,
        exe: str,
        call: Callable[[Path], None],
    ):
        fake_slurm.timeout_command(exe)

        with pytest.raises(subprocess.TimeoutExpired):
            call(tmp_path)
