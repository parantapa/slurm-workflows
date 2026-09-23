"""Helpers for driving a real PilotWorker in-process."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from ds_service_client import DsServiceClient
from slurm_workflows.slurm_pilot_worker import PilotWorker


# A BaseException, not an Exception, because `main()` catches every Exception.
# See docs/how-to-run-tests.md, "Notes for future changes".
class StopWorker(BaseException):
    """Breaks the worker's otherwise-infinite main loop."""


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
    # by the first worker on a host,
    # which is what the tests look at.
    monitor_interval: float = 60.0,
) -> PilotWorker:
    """A real worker against a real server."""
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
    """Run the worker's real main loop until it completes `expect_tasks` tasks."""
    # `_StoppingClient` forwards everything it does not override,
    # so it satisfies the worker's use of the client without subclassing it.
    worker.client = cast(DsServiceClient, _StoppingClient(worker.client, expect_tasks))
    # The tasks must arrive, before the call or from another thread.
    # The worker answers an empty queue with a sleep and another request,
    # so a worker that never gets `expect_tasks` tasks
    # spins until the hang guard fires.
    try:
        worker.main()
    except StopWorker:
        pass


def poll_worker(worker: PilotWorker, polls: int) -> int:
    """Run the worker's real main loop for `polls` fetches, and count them."""
    # For the empty-queue case, where `run_worker` never stops.
    # See docs/how-to-run-tests.md, "Notes for future changes".
    # The count tells a loop that gave up early from one that kept polling.
    client = _IdlingClient(worker.client, polls)
    worker.client = cast(DsServiceClient, client)
    try:
        worker.main()
    except StopWorker:
        pass
    # The last call raised `StopWorker` instead of fetching.
    return client.polls - 1
