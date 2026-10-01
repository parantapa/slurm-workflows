"""Shared test fixtures."""

# ds-service is real and Slurm is mocked.
# See docs/how-to-run-tests.md, "What is real and what is mocked".

from __future__ import annotations

import os
import sys
import signal
import threading
from pathlib import Path
from types import FrameType, SimpleNamespace
from typing import Callable, Generator, Iterator, NoReturn
from dataclasses import dataclass
from contextlib import AbstractContextManager, contextmanager

import pynvml
import pytest

# Before the import below, which would otherwise load the plugin
# ahead of pytest's assertion rewriting.
pytest.register_assert_rewrite("slurm_workflows.testing")

from slurm_workflows.slurm_pilot_worker import PilotWorker  # noqa: E402
from slurm_workflows.testing import make_worker, run_worker  # noqa: E402

# The server, the fake Slurm, the executor and the in-process worker helpers.
# Other packages that build on this one load the same plugin.
pytest_plugins = ["slurm_workflows.testing"]

# Test-support modules (for example, support_actor) must be importable by name,
# both for `import` in the test modules
# and for the worker's importlib-based actor lookup.
sys.path.insert(0, str(Path(__file__).parent))


# --------------------------------------------------------------------------
# Hang guards
# --------------------------------------------------------------------------
#
# Why a wall-clock alarm bounds every test:
# see docs/how-to-run-tests.md, "Notes for future changes".


@contextmanager
def _time_limit(seconds: float, message: str) -> Iterator[None]:
    def on_alarm(signum: int, frame: FrameType | None) -> NoReturn:
        raise TimeoutError(message)

    previous = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        # This disarms any outer alarm as well.
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def time_limit() -> Callable[[float, str], AbstractContextManager[None]]:
    """Bound a block that can spin forever if the code under test regresses.

    The block raises `TimeoutError(message)` once `seconds` pass.
    The end of the block also ends the 60 s hang guard,
    so the rest of the test runs unguarded.
    """

    return _time_limit


@pytest.fixture(autouse=True)
def _hang_guard() -> Generator[None]:
    """Fail any test that runs past 60 s, so that no single test can hang the suite."""

    with _time_limit(60.0, "test exceeded its 60s time limit"):
        yield


@pytest.fixture(autouse=True)
def _restore_environ() -> Generator[None]:
    """Put `os.environ` back after each test."""

    # `PilotWorker.__init__` writes `DS_SERVER_ADDRESS`, `PILOT_WORKER_ID`
    # and the `PILOT_JOB_*` variables, and undoes none of them.
    # Left in place, a dead server's address becomes the address
    # that `DsServiceClient()` falls back to in every later test.
    env = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(env)


# --------------------------------------------------------------------------
# GPUs
# --------------------------------------------------------------------------


@dataclass
class FakeGpu:
    """One GPU as `FakeNvml` reports it.

    A measurement set to None is one the GPU does not support.
    """

    name: str = "NVIDIA A100-SXM4-80GB"
    uuid: str = "GPU-aaaa"
    memory_total: int | None = 80 * 1024**3
    memory_used: int = 1024**3
    utilization: int | None = 45


class FakeNvml:
    """Stands in for the functions of `pynvml` that the GPU monitor calls.

    It lists the GPUs in `gpus`.
    The list is empty until a test adds a GPU,
    and an empty list reads as a node whose driver finds no GPU.
    With `no_driver` set, `nvmlInit` fails as it does on a node with no driver.
    It raises the real `pynvml` errors.
    """

    FUNCTIONS = [
        "nvmlInit",
        "nvmlShutdown",
        "nvmlDeviceGetCount",
        "nvmlDeviceGetHandleByIndex",
        "nvmlDeviceGetName",
        "nvmlDeviceGetUUID",
        "nvmlDeviceGetMemoryInfo",
        "nvmlDeviceGetUtilizationRates",
    ]

    def __init__(self) -> None:
        self.gpus: list[FakeGpu] = []
        self.no_driver = False
        # Open sessions: the `nvmlInit` calls minus the `nvmlShutdown` calls.
        self.sessions = 0

    def _check(self) -> None:
        if self.sessions <= 0:
            raise pynvml.NVMLError(pynvml.NVML_ERROR_UNINITIALIZED)

    def nvmlInit(self) -> None:
        if self.no_driver:
            raise pynvml.NVMLError(pynvml.NVML_ERROR_LIBRARY_NOT_FOUND)
        self.sessions += 1

    def nvmlShutdown(self) -> None:
        self._check()
        self.sessions -= 1

    def nvmlDeviceGetCount(self) -> int:
        self._check()
        return len(self.gpus)

    # A handle is the index, which is all the fake needs.

    def nvmlDeviceGetHandleByIndex(self, index: int) -> int:
        self._check()
        if not 0 <= index < len(self.gpus):
            raise pynvml.NVMLError(pynvml.NVML_ERROR_INVALID_ARGUMENT)
        return index

    def nvmlDeviceGetName(self, handle: int) -> str:
        self._check()
        return self.gpus[handle].name

    def nvmlDeviceGetUUID(self, handle: int) -> str:
        self._check()
        return self.gpus[handle].uuid

    def nvmlDeviceGetMemoryInfo(self, handle: int) -> SimpleNamespace:
        self._check()
        gpu = self.gpus[handle]
        if gpu.memory_total is None:
            raise pynvml.NVMLError(pynvml.NVML_ERROR_NOT_SUPPORTED)
        return SimpleNamespace(
            total=gpu.memory_total,
            used=gpu.memory_used,
            free=gpu.memory_total - gpu.memory_used,
        )

    def nvmlDeviceGetUtilizationRates(self, handle: int) -> SimpleNamespace:
        self._check()
        gpu = self.gpus[handle]
        if gpu.utilization is None:
            raise pynvml.NVMLError(pynvml.NVML_ERROR_NOT_SUPPORTED)
        return SimpleNamespace(gpu=gpu.utilization, memory=0)


@pytest.fixture(autouse=True)
def fake_nvml(monkeypatch: pytest.MonkeyPatch) -> FakeNvml:
    """Stand in for NVML in every test, with no GPU until the test adds one.

    So a machine that has GPUs runs the suite as one that has none.
    It patches `pynvml` in this process only.
    A worker process that a test starts reads the real NVML.
    """
    fake = FakeNvml()
    for name in FakeNvml.FUNCTIONS:
        monkeypatch.setattr(pynvml, name, getattr(fake, name))
    return fake


# --------------------------------------------------------------------------
# Script inspection
# --------------------------------------------------------------------------


def _srun_lines(script: str) -> list[str]:
    """The `srun` command lines in a generated script, in the order rendered."""

    # Strip before the match,
    # because the non-batch worker script indents its `srun` calls
    # inside the shell `if` that chooses between them.
    # Test each line rather than the whole text,
    # since a path written into the script can itself contain "srun".
    return [ln.strip() for ln in script.splitlines() if ln.strip().startswith("srun")]


@pytest.fixture
def srun_lines() -> Callable[[str], list[str]]:
    """Extract the `srun` command lines from a generated script."""

    return _srun_lines


@pytest.fixture
def setup_script() -> str:
    """A setup script body, as define_job_group expects."""
    return "module load gcc/14.2.0\nexport TEST_SETUP=1\n"


# --------------------------------------------------------------------------
# Map tasks and in-process workers
# --------------------------------------------------------------------------


# See the developer notes, "A map task builds a client of its own".
@pytest.fixture
def map_task_env(monkeypatch: pytest.MonkeyPatch, ds_service_address: str) -> None:
    """What a worker puts in the environment, for a task driven without one."""
    monkeypatch.setenv("DS_SERVER_ADDRESS", ds_service_address)
    monkeypatch.setenv("PILOT_WORKER_ID", "test-worker.42.testhost.4242")


# Why a real worker runs in a thread:
# see docs/how-to-run-tests.md, "Notes for future changes".
@pytest.fixture
def worker_thread(
    ds_service_address: str, tmp_path: Path
) -> Generator[Callable[..., None]]:
    """Run a real worker in a thread until it completes `expect_tasks`."""
    started: list[tuple[PilotWorker, threading.Thread]] = []

    def start(
        expect_tasks: int,
        group: str = "cpu",
        name: str = "worker-0",
        actor_class_name: str = "",
    ) -> None:
        worker = make_worker(
            ds_service_address,
            tmp_path / name,
            group=group,
            name=name,
            actor_class_name=actor_class_name,
        )
        thread = threading.Thread(
            target=run_worker, args=(worker, expect_tasks), daemon=True
        )
        thread.start()
        started.append((worker, thread))

    yield start

    for worker, thread in started:
        # Well inside the 60 s alarm on every test,
        # so a stuck worker fails here, not at the alarm.
        thread.join(timeout=30)
        worker.close()
