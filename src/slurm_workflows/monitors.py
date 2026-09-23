"""Background sampling of a compute node and of a Slurm job.

One elected worker per node and one per job runs these threads.
Each thread appends to a `ds-service` time series,
one series per measurement per subject.
`docs/reference/swtop.md` says what the readings mean.
"""

from __future__ import annotations

import time
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable

import psutil

from ds_service_client import DsServiceClient

DEFAULT_MONITOR_INTERVAL_S: float = 5.0

# Where the cgroup v2 files of the current process live.
# Slurm accounts the whole job on this node here.
CGROUP_ROOT = Path("/sys/fs/cgroup")

# Node-local scratch, which a job can fill.
HOST_FILESYSTEMS = {"dev_shm": "/dev/shm", "tmp": "/tmp"}

# Time series keys, as `<prefix><subject>`.
# The subject is the hostname for a host and the job id for a job.
HOST_SERIES = {
    "free_memory": "host_free_memory:",  # bytes
    "load_average": "host_load_average:",  # 1-minute load average
    "dev_shm_used": "host_dev_shm_used:",  # percent of /dev/shm in use
    "tmp_used": "host_tmp_used:",  # percent of /tmp in use
}
JOB_SERIES = {
    "memory": "slurm_job_memory:",  # bytes, the cgroup's own total
    "cpu": "slurm_job_cpu:",  # cores in use, averaged over the interval
}


def sample_host() -> dict[str, float]:
    """One reading of this node: free memory, load, and scratch usage.

    The reading omits a scratch path that `psutil.disk_usage` cannot read,
    such as one that does not exist.
    It does not report that path as zero.
    A path that is not a mount point of its own
    reports the filesystem that holds it.
    """
    values = {
        "free_memory": float(psutil.virtual_memory().available),
        "load_average": float(psutil.getloadavg()[0]),
    }

    for name, path in HOST_FILESYSTEMS.items():
        try:
            values[f"{name}_used"] = float(psutil.disk_usage(path).percent)
        except OSError:
            continue

    return values


class CgroupSampler:
    """Total memory and CPU of everything in this process's cgroup.

    CPU is cores in use, averaged since the previous sample,
    so the first sample of a run reports 0.
    """

    def __init__(self, root: Path = CGROUP_ROOT) -> None:
        self.root = root
        self._last: tuple[float, float] | None = None

    def sample(self) -> dict[str, float]:
        """One reading: total memory in bytes, and cores used since the last."""
        now = time.monotonic()
        # The cgroup's own accounting comes first,
        # because it covers every process Slurm put in the job,
        # including ones the worker never started.
        reading = self._read_cgroup()
        if reading is None:
            reading = self._read_processes()
        memory, cpu_seconds = reading

        cores = 0.0
        if self._last is not None:
            last_now, last_cpu = self._last
            elapsed = now - last_now
            if elapsed > 0:
                # Clamped: a recreated cgroup restarts the counter.
                cores = max(0.0, (cpu_seconds - last_cpu) / elapsed)
        self._last = (now, cpu_seconds)

        return {"memory": memory, "cpu": cores}

    def _read_cgroup(self) -> tuple[float, float] | None:
        """Memory in bytes and cumulative CPU seconds from cgroup v2, or None."""
        try:
            memory = float((self.root / "memory.current").read_text().strip())
            cpu_stat = (self.root / "cpu.stat").read_text()
        except (OSError, ValueError):
            return None

        for line in cpu_stat.splitlines():
            key, _, value = line.partition(" ")
            if key == "usage_usec":
                try:
                    return memory, float(value) / 1e6
                except ValueError:
                    return None
        return None

    def _read_processes(self) -> tuple[float, float]:
        """Memory and CPU seconds summed over the processes in the cgroup."""
        # Only a fallback: summed RSS counts shared pages once per process.
        # The root cgroup of a systemd host holds no process with an address space,
        # so it reads as zero, and this sums the tree of this process instead.
        memory, cpu_seconds = self._sum(self._cgroup_processes())
        if memory > 0.0:
            return memory, cpu_seconds
        return self._sum(self._own_tree())

    @staticmethod
    def _sum(procs: list[psutil.Process]) -> tuple[float, float]:
        """Total memory in bytes and total CPU seconds over `procs`."""
        memory = 0.0
        cpu_seconds = 0.0

        for proc in procs:
            try:
                with proc.oneshot():
                    memory += float(proc.memory_info().rss)
                    times = proc.cpu_times()
                    cpu_seconds += times.user + times.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # A process that exited between the listing and the read,
                # or one this user cannot read.
                continue

        return memory, cpu_seconds

    def _cgroup_processes(self) -> list[psutil.Process]:
        """The processes `cgroup.procs` names, where that file is readable."""
        try:
            pids = [
                int(line)
                for line in (self.root / "cgroup.procs").read_text().split()
                if line
            ]
        except (OSError, ValueError):
            return []

        procs = []
        for pid in pids:
            try:
                procs.append(psutil.Process(pid))
            except psutil.NoSuchProcess:
                continue
        return procs

    @staticmethod
    def _own_tree() -> list[psutil.Process]:
        """This process and every descendant of it."""
        this = psutil.Process()
        return [this, *this.children(recursive=True)]


class Monitor(threading.Thread):
    """Appends one sampler's readings to `ds-service`, on a timer.

    Runs as a daemon thread.
    `prefixes` maps each key the sampler returns to its series prefix.
    `interval` is the time between readings, in seconds.
    A failed reading is logged, and the thread carries on.
    """

    def __init__(
        self,
        client: DsServiceClient,
        subject: str,
        prefixes: dict[str, str],
        sampler: Callable[[], dict[str, float]],
        interval: float = DEFAULT_MONITOR_INTERVAL_S,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(daemon=True, name=f"monitor:{subject}")
        self.client = client
        self.subject = subject
        self.prefixes = prefixes
        self.sampler = sampler
        self.interval = interval
        self.logger = logger or logging.getLogger("worker_process")
        self._stopping = threading.Event()

    def run(self) -> None:
        while True:
            try:
                self.append_sample()
            except Exception:
                # A failed sample leaves a gap, not a stop.
                # See "Monitoring" in the developer notes.
                self.logger.exception("Monitor %s failed to sample", self.subject)

            if self._stopping.wait(self.interval):
                return

    def append_sample(self) -> None:
        """Take one reading and append it to this subject's series."""
        values = self.sampler()
        # One timestamp for the whole reading,
        # so the series of one subject line up point for point.
        stamp = datetime.now(timezone.utc).isoformat()

        for name, value in values.items():
            self.client.time_series_append(
                f"{self.prefixes[name]}{self.subject}", float(value), stamp
            )

    def stop(self, timeout: float = 10.0) -> None:
        """Ask the thread to finish its wait and end, then wait for it to end.

        Waits at most `timeout` seconds,
        and returns then even if the thread has not ended.
        Idempotent, and safe on a thread that never started.
        """
        self._stopping.set()
        if self.is_alive():
            self.join(timeout)


def start_host_monitor(
    client: DsServiceClient,
    hostname: str,
    interval: float = DEFAULT_MONITOR_INTERVAL_S,
    logger: logging.Logger | None = None,
) -> Monitor:
    """Start sampling this node, and return the running thread.

    `interval` is the time between readings, in seconds.
    """
    monitor = Monitor(
        client=client,
        subject=hostname,
        prefixes=HOST_SERIES,
        sampler=sample_host,
        interval=interval,
        logger=logger,
    )
    monitor.start()
    return monitor


def start_slurm_job_monitor(
    client: DsServiceClient,
    slurm_job_id: str | int,
    interval: float = DEFAULT_MONITOR_INTERVAL_S,
    logger: logging.Logger | None = None,
) -> Monitor:
    """Start sampling this job's cgroup, and return the running thread.

    `interval` is the time between readings, in seconds.
    """
    monitor = Monitor(
        client=client,
        subject=str(slurm_job_id),
        prefixes=JOB_SERIES,
        sampler=CgroupSampler().sample,
        interval=interval,
        logger=logger,
    )
    monitor.start()
    return monitor
