"""Pytest fixtures and helpers for testing code built on slurm-workflows.

This module is a pytest plugin.
Load it from a `conftest.py`:

    pytest_plugins = ["slurm_workflows.testing"]

It gives a real `ds-service` server, a fake Slurm,
an executor wired to both, and helpers that run a real worker in-process.
It imports `pytest`, which is not a dependency of `slurm-workflows`,
so import it only from a test suite.
"""

# ds-service is real and Slurm is mocked.
# See docs/how-to-run-tests.md, "What is real and what is mocked".

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Callable, Generator, cast
from dataclasses import dataclass

import pytest
from ds_service_client import DsServiceClient, DsServiceServer

from . import slurm_utils
from .slurm_pilot_executor import SlurmPilotExecutor
from .slurm_pilot_worker import PilotWorker

# A change to a name here can break the suite of a package
# that builds on this one, such as `slurm-workflows-optimize`.
# See "The public surface other packages build on" in the developer notes.
__all__ = [
    "FakeSlurm",
    "Submission",
    "StopWorker",
    "ds_service_address",
    "ds_client",
    "fake_slurm",
    "executor",
    "pilot_jobs",
    "make_worker",
    "run_worker",
    "poll_worker",
    "run_until_restart",
]


# --------------------------------------------------------------------------
# ds-service (real)
# --------------------------------------------------------------------------


@pytest.fixture
def ds_service_address() -> Generator[str]:
    """Run a private ds-service for one test and yield its address.

    The test skips when no `ds-service` executable is found.
    """
    # A fresh in-memory server per test starts in about 10 ms.
    # `DsServiceServer` owns the lifecycle, not this fixture:
    # it finds the binary, picks a free port,
    # waits for the socket and terminates the process.
    # This fixture only chooses the interface to bind,
    # and translates a missing binary into a skip.
    # `DsServiceServer` takes an interface name rather than an address.
    # Loopback keeps a test's server unreachable from outside the machine.
    try:
        server = DsServiceServer(interface="lo")
    except FileNotFoundError:
        pytest.skip(
            "ds-service executable not found; "
            "put `ds-service` on PATH or point DS_SERVICE_BIN at it"
        )

    try:
        # A poll of the TCP socket, not an RPC probe.
        # A failed first RPC puts the gRPC channel into a ~1s reconnect backoff,
        # and an RPC probe makes the suite ~100x slower.
        server.wait_until_ready(timeout=10)
        yield server.address
    finally:
        server.close()


@pytest.fixture
def ds_client(ds_service_address: str) -> Generator[DsServiceClient]:
    """A directly usable client against the test's ds-service."""
    client = DsServiceClient(ds_service_address)
    yield client
    client.close()


# --------------------------------------------------------------------------
# Slurm (mocked)
# --------------------------------------------------------------------------


@dataclass
class Submission:
    """One captured `sbatch` call."""

    job_id: int
    script_path: Path
    script_text: str
    env: dict[str, str]

    @property
    def job_name(self) -> str:
        """The `--job-name` the captured script sets, without quotes.

        The property raises `AssertionError` when the script sets none.
        """
        for line in self.script_text.splitlines():
            if line.startswith("#SBATCH --job-name"):
                return line.split(maxsplit=2)[2].strip('"')
        raise AssertionError("no --job-name in submitted script")

    @property
    def sbatch_directives(self) -> list[str]:
        """`#SBATCH` lines, minus the name and output ones the library adds."""
        out: list[str] = []
        for line in self.script_text.splitlines():
            if not line.startswith("#SBATCH "):
                continue
            body = line[len("#SBATCH ") :]
            if body.startswith(("--job-name", "--output")):
                continue
            out.append(body)
        return out


class FakeSlurm:
    """Stands in for the `subprocess` module inside `slurm_utils`."""

    def __init__(self) -> None:
        self.submissions: list[Submission] = []
        self.running_job_ids: list[int] = []
        self.cancelled_job_ids: list[int] = []
        self.next_job_id = 1000
        self.fail: dict[str, tuple[int, str, str]] = {}
        self.sbatch_stdout_override: str | None = None

    # -- failure injection --------------------------------------------------

    def fail_command(
        self, exe: str, returncode: int = 1, stdout: str = "", stderr: str = "boom"
    ) -> None:
        """Make future calls to `exe` raise CalledProcessError."""
        self.fail[exe] = (returncode, stdout, stderr)

    # -- the subprocess surface --------------------------------------------

    def run(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """Answer an `sbatch`, `squeue` or `scancel` call.

        Any other command fails the test.
        """
        exe = Path(cmd[0]).name

        if exe in self.fail:
            returncode, stdout, stderr = self.fail[exe]
            raise subprocess.CalledProcessError(returncode, cmd, stdout, stderr)

        if exe == "sbatch":
            return self._sbatch(cmd, **kwargs)
        if exe == "squeue":
            return self._squeue(cmd)
        if exe == "scancel":
            return self._scancel(cmd)

        raise AssertionError(f"unexpected command in test: {cmd!r}")

    # `run` covers only the three Slurm commands.
    # Every other attribute, such as an exception type,
    # comes from the real `subprocess`.
    def __getattr__(self, name: str) -> Any:
        return getattr(subprocess, name)

    # -- individual commands ------------------------------------------------

    def _sbatch(
        self, cmd: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        script_path = Path(cmd[1])
        job_id = self.next_job_id
        self.next_job_id += 1

        self.submissions.append(
            Submission(
                job_id=job_id,
                script_path=script_path,
                script_text=script_path.read_text(),
                env=dict(kwargs.get("env") or {}),
            )
        )
        self.running_job_ids.append(job_id)

        stdout = self.sbatch_stdout_override
        if stdout is None:
            stdout = f"Submitted batch job {job_id}\n"
        return subprocess.CompletedProcess(cmd, 0, stdout, "")

    def _squeue(self, cmd: list[str]) -> subprocess.CompletedProcess[str]:
        stdout = "".join(f"{job_id}\n" for job_id in self.running_job_ids)
        return subprocess.CompletedProcess(cmd, 0, stdout, "")

    def _scancel(self, cmd: list[str]) -> subprocess.CompletedProcess[str]:
        for arg in cmd[1:]:
            if arg.startswith("-"):
                continue
            job_id = int(arg)
            self.cancelled_job_ids.append(job_id)
            if job_id in self.running_job_ids:
                self.running_job_ids.remove(job_id)
        return subprocess.CompletedProcess(cmd, 0, "", "")


@pytest.fixture
def fake_slurm(monkeypatch: pytest.MonkeyPatch) -> Generator[FakeSlurm]:
    """Intercept Slurm commands, so a test needs no cluster."""
    fake = FakeSlurm()
    monkeypatch.setattr(slurm_utils, "subprocess", fake)
    # `@cache` wraps `get_clean_environ`.
    # Clear the cache so each test sees its own environment.
    slurm_utils.get_clean_environ.cache_clear()
    yield fake
    slurm_utils.get_clean_environ.cache_clear()


# --------------------------------------------------------------------------
# Executor
# --------------------------------------------------------------------------


@pytest.fixture
def executor(
    ds_service_address: str, fake_slurm: FakeSlurm, tmp_path: Path
) -> Generator[SlurmPilotExecutor]:
    """An executor wired to the real server and the fake Slurm."""
    ex = SlurmPilotExecutor(
        name="testex", server_address=ds_service_address, work_dir=tmp_path / "work"
    )
    yield ex
    ex.close()


@pytest.fixture
def pilot_jobs(executor: SlurmPilotExecutor) -> Callable[..., None]:
    """Declare a pilot job for each named job group, as any test that waits needs."""

    # `as_completed` refuses a queue this executor never started a worker for.
    # It does so even where an in-process worker drains the queue,
    # or where a test claims the tasks itself.
    # The Slurm job stands in for the allocation,
    # and the in-process worker or the test stands in for the process inside it.

    def declare(*names: str) -> None:
        for name in names:
            executor.define_job_group(name, [])
            executor.scale_jobs(name, 1)

    return declare


# --------------------------------------------------------------------------
# In-process workers
# --------------------------------------------------------------------------


# A BaseException, not an Exception, because `main()` catches every Exception.
# See docs/how-to-run-tests.md, "Notes for future changes".
class StopWorker(BaseException):
    """Breaks the worker's main loop, which otherwise runs until a restart request."""


# Both wrappers forward everything they do not override,
# so each satisfies the worker's use of the client without subclassing it.
# The helpers below cast them to `DsServiceClient` for that reason.
class _StoppingClient:
    """Wraps a real client, and raises StopWorker after N calls to `task_done`."""

    def __init__(self, inner: DsServiceClient, limit: int) -> None:
        self._inner = inner
        self._limit = limit
        self.completed = 0

    def task_done(
        self, task_id: str, worker_id: str, output: bytes, failed: bool = False
    ) -> None:
        result = self._inner.task_done(task_id, worker_id, output, failed=failed)
        self.completed += 1
        if self.completed >= self._limit:
            raise StopWorker
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _IdlingClient:
    """Wraps a real client, and raises StopWorker after N `task_get` calls."""

    def __init__(self, inner: DsServiceClient, limit: int) -> None:
        self._inner = inner
        self._limit = limit
        self.polls = 0

    def task_get(self, worker_id: str, queue: str | list[str]):
        self.polls += 1
        if self.polls > self._limit:
            raise StopWorker
        # The real call,
        # so an empty queue answers with the server's own `NoTaskAvailable`
        # rather than one this double invented.
        return self._inner.task_get(worker_id, queue)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def make_worker(
    address: str,
    work_dir: Path,
    group: str = "cpu",
    name: str = "worker-0",
    actor_class_name: str = "",
    slurm_job_id: int = 42,
    hostname: str = "testhost",
    # Long, so the only sample is the one taken at startup
    # by the first worker of each job on a host.
    # The tests look at that sample.
    monitor_interval: float = 60.0,
) -> PilotWorker:
    """A real worker against a real server.

    The worker's constructor publishes its identity to the server,
    and sets `PILOT_JOB_NAME`, `PILOT_JOB_GROUP`, `PILOT_WORKER_ID`
    and `DS_SERVER_ADDRESS` in `os.environ`.
    The caller ends it with `close()`,
    which stops its monitors, publishes its exit,
    and closes its client and its actor.
    """
    return PilotWorker(
        group=group,
        name=name,
        actor_class_name=actor_class_name,
        server_address=address,
        work_dir=Path(work_dir),
        slurm_job_id=slurm_job_id,
        hostname=hostname,
        pid=4242,
        monitor_interval=monitor_interval,
    )


def run_worker(worker: PilotWorker, expect_tasks: int) -> None:
    """Run the worker's real main loop until it completes `expect_tasks` tasks.

    The tasks must be on the queue before the call, or arrive from another thread.
    The worker answers an empty queue with a sleep and another request.
    As a result, a worker that never gets `expect_tasks` tasks
    spins until something else ends the test, such as a timeout the suite sets.
    The plugin sets none.
    """
    worker.client = cast(DsServiceClient, _StoppingClient(worker.client, expect_tasks))
    try:
        worker.main()
    except StopWorker:
        pass


def poll_worker(worker: PilotWorker, polls: int) -> int:
    """Run the worker's real main loop for `polls` fetches, and count them.

    It suits the empty-queue case, where `run_worker` never stops.
    The count tells a loop that gave up early from one that kept polling.
    For a worker that returns for a restart, the count is one short.
    """
    client = _IdlingClient(worker.client, polls)
    worker.client = cast(DsServiceClient, client)
    try:
        worker.main()
    except StopWorker:
        pass
    # The last call raised `StopWorker` instead of fetching.
    return client.polls - 1


def run_until_restart(worker: PilotWorker, max_polls: int) -> int:
    """Run the worker's real main loop until a restart, and count its fetches.

    A worker that makes more than `max_polls` fetches
    fails the test with `AssertionError`, rather than spin.
    """
    client = _IdlingClient(worker.client, max_polls)
    worker.client = cast(DsServiceClient, client)
    try:
        worker.main()
    except StopWorker:
        raise AssertionError(
            f"the worker did not return for a restart within {max_polls} fetches"
        ) from None
    return client.polls
