"""Background sampling of a node, of the Slurm job on it, and of that job's GPUs.

Each thread appends to a `ds-service` time series,
one series per measurement per subject.
`docs/reference/swtop.md` says what the readings mean.
`docs/reference/what-a-run-publishes.md` lists every key they write.
"""

from __future__ import annotations

import json
import time
import logging
import threading
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Mapping

import psutil
import pynvml

from ds_service_client import DsServiceClient

DEFAULT_MONITOR_INTERVAL_S: float = 5.0

# Where the cgroup v2 files of the current process live.
# Slurm accounts the whole job on this node here.
CGROUP_ROOT = Path("/sys/fs/cgroup")

# Node-local scratch, which a job can fill.
HOST_FILESYSTEMS = {"dev_shm": "/dev/shm", "tmp": "/tmp"}

# Time series keys, as `<prefix><subject>`.
# The subject is the hostname for a host,
# and `<job-id>:<hostname>` for the part of a job on one node.
HOST_SERIES = {
    "free_memory": "host_free_memory:",  # bytes
    "load_average": "host_load_average:",  # 1-minute load average
    "dev_shm_used": "host_dev_shm_used:",  # percent of /dev/shm in use
    "tmp_used": "host_tmp_used:",  # percent of /tmp in use
}
JOB_SERIES = {
    # The cgroup's own total on one node, or summed RSS where that cannot be read.
    "memory": "slurm_job_memory:",  # bytes
    "cpu": "slurm_job_cpu:",  # cores in use, averaged over the interval
}

# The subject of a GPU series is `<job-id>:<hostname>:<gpu-index>`.
GPU_SERIES = {
    "memory_used": "slurm_job_gpu_memory_used:",  # bytes
    "memory_free": "slurm_job_gpu_memory_free:",  # bytes
    # Percent of the driver's last sample period, 1/6 s to 1 s,
    # in which one or more kernels ran on the GPU.
    "utilization": "slurm_job_gpu_utilization:",
}

# A map key per GPU, as `<prefix><job-id>:<hostname>:<gpu-index>`,
# with what a time series cannot hold: the GPU's type, as text.
GPU_INFO_PREFIX = "slurm_job_gpu_info:"

# Splits the job id from the hostname in the subject of a job series,
# and the hostname from the GPU index in the subject of a GPU series.
# Neither a Slurm job id nor a hostname holds a colon.
JOB_SUBJECT_SEPARATOR = ":"


def job_subject(slurm_job_id: str | int, hostname: str) -> str:
    """The subject of the series that sample one job on one node."""
    return f"{slurm_job_id}{JOB_SUBJECT_SEPARATOR}{hostname}"


def split_job_subject(subject: str) -> tuple[str, str]:
    """The job id and the hostname in `subject`.

    The hostname is "" for a subject with no separator in it.
    """
    slurm_job_id, _, hostname = subject.partition(JOB_SUBJECT_SEPARATOR)
    return slurm_job_id, hostname


def gpu_subject(slurm_job_id: str | int, hostname: str, index: str | int) -> str:
    """The subject of the series that sample one GPU of one job on one node."""
    return f"{job_subject(slurm_job_id, hostname)}{JOB_SUBJECT_SEPARATOR}{index}"


def split_gpu_subject(subject: str) -> tuple[str, str, str]:
    """The job id, the hostname and the GPU index in `subject`."""
    rest, _, index = subject.rpartition(JOB_SUBJECT_SEPARATOR)
    slurm_job_id, hostname = split_job_subject(rest)
    return slurm_job_id, hostname, index


@dataclass
class GpuReading:
    """One reading of one GPU, as NVML reports it.

    `values` maps the keys of `GPU_SERIES` to numbers.
    It leaves out a measurement the GPU does not support,
    such as the utilization of a MIG instance.
    `index` is NVML's index of the GPU.
    `memory_total` is in bytes, or None where it is not supported.
    """

    index: str
    uuid: str
    name: str
    memory_total: float | None = None
    values: dict[str, float] = field(default_factory=dict)

    @property
    def info(self) -> dict[str, str | float | None]:
        """What `GpuMonitor` publishes about the GPU under `GPU_INFO_PREFIX`."""
        return {"name": self.name, "uuid": self.uuid, "memory_total": self.memory_total}


@contextmanager
def nvml_session() -> Iterator[None]:
    """Initialize NVML for the length of the block, and shut it down after.

    Raises `pynvml.NVMLError` where NVML cannot start,
    as on a node with no NVIDIA driver.
    NVML counts its sessions, so they can nest and overlap across threads.
    """
    pynvml.nvmlInit()
    try:
        yield
    finally:
        pynvml.nvmlShutdown()


def _if_supported(read: Callable[[Any], Any], handle: Any) -> Any | None:
    """What `read` returns for the GPU `handle`, or None where it is not supported."""
    try:
        return read(handle)
    except pynvml.NVMLError as e:
        # The code, and not `NVMLError_NotSupported`,
        # since `pynvml` builds its error classes and their `value` at import,
        # where a type checker cannot see them.
        if getattr(e, "value", None) == pynvml.NVML_ERROR_NOT_SUPPORTED:
            return None
        raise


def query_gpus() -> list[GpuReading]:
    """One reading of every GPU NVML lists for this process.

    Call it inside an `nvml_session`.
    NVML ignores `CUDA_VISIBLE_DEVICES`.
    Where Slurm constrains devices, it lists the GPUs of the job step on this node.
    Elsewhere it lists every GPU on the node.
    Raises `pynvml.NVMLError` if NVML cannot read a GPU.
    """
    readings = []
    for index in range(pynvml.nvmlDeviceGetCount()):
        handle = pynvml.nvmlDeviceGetHandleByIndex(index)
        gpu = GpuReading(
            index=str(index),
            uuid=pynvml.nvmlDeviceGetUUID(handle),
            name=pynvml.nvmlDeviceGetName(handle),
        )

        memory = _if_supported(pynvml.nvmlDeviceGetMemoryInfo, handle)
        if memory is not None:
            gpu.memory_total = float(memory.total)
            gpu.values["memory_used"] = float(memory.used)
            gpu.values["memory_free"] = float(memory.free)

        utilization = _if_supported(pynvml.nvmlDeviceGetUtilizationRates, handle)
        if utilization is not None:
            gpu.values["utilization"] = float(utilization.gpu)

        readings.append(gpu)
    return readings


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

        # The kernel counts CPU time up from zero, so a rate needs the last reading.
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

        procs: list[psutil.Process] = []
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


class BaseMonitor(threading.Thread):
    """Calls `append_sample` on a timer, until stopped.

    Runs as a daemon thread.
    `interval` is the time between readings, in seconds.
    A failed reading is logged, and the thread carries on.
    A subclass defines `append_sample`.
    """

    def __init__(
        self,
        client: DsServiceClient,
        subject: str,
        interval: float = DEFAULT_MONITOR_INTERVAL_S,
        logger: logging.Logger | None = None,
    ) -> None:
        # A daemon, so a monitor never holds open a worker
        # that Slurm kills at its time limit.
        super().__init__(daemon=True, name=f"monitor:{subject}")
        self.client = client
        self.subject = subject
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
        """Take one reading and publish it."""
        raise NotImplementedError

    def stop(self, timeout: float = 10.0) -> None:
        """Ask the thread to finish its wait and end, then wait for it to end.

        Waits at most `timeout` seconds,
        and returns then even if the thread has not ended.
        Idempotent, and safe on a thread that never started.
        """
        self._stopping.set()
        if self.is_alive():
            self.join(timeout)


class Monitor(BaseMonitor):
    """Appends one sampler's readings to `ds-service`, on a timer.

    `prefixes` maps each key the sampler returns to its series prefix.
    """

    def __init__(
        self,
        client: DsServiceClient,
        subject: str,
        prefixes: Mapping[str, str],
        sampler: Callable[[], dict[str, float]],
        interval: float = DEFAULT_MONITOR_INTERVAL_S,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(client, subject, interval, logger)
        self.prefixes = prefixes
        self.sampler = sampler

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


class GpuMonitor(BaseMonitor):
    """Appends a reading of each GPU a job can see on this node, on a timer.

    `subject` is the job's subject, `<job-id>:<hostname>`.
    Each GPU gets series of its own, under `<job-id>:<hostname>:<gpu-index>`.
    The GPU's type goes in the map under `GPU_INFO_PREFIX`,
    written when the monitor first sees the GPU,
    and again only if it changes.
    The thread holds an `nvml_session` for as long as it runs.
    A call to `append_sample` from outside the thread needs a session of its own.
    """

    def __init__(
        self,
        client: DsServiceClient,
        subject: str,
        interval: float = DEFAULT_MONITOR_INTERVAL_S,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(client, subject, interval, logger)
        # The base class names the thread after the subject,
        # which the job monitor shares.
        self.name = f"gpu-monitor:{subject}"
        # What this monitor last published about each GPU, by index.
        self._published: dict[str, dict[str, str | float | None]] = {}

    def run(self) -> None:
        # One session for the whole run, not one per reading,
        # since starting NVML is far slower than reading it.
        try:
            with nvml_session():
                super().run()
        except pynvml.NVMLError:
            # Only starting or stopping NVML gets here.
            # `BaseMonitor.run` catches what a reading raises.
            self.logger.exception("Monitor %s cannot use NVML", self.name)

    def append_sample(self) -> None:
        """Read every GPU, and append each reading to that GPU's series."""
        readings = query_gpus()
        # One timestamp for every GPU, as for the measurements of one subject.
        stamp = datetime.now(timezone.utc).isoformat()

        for gpu in readings:
            subject = f"{self.subject}{JOB_SUBJECT_SEPARATOR}{gpu.index}"
            if self._published.get(gpu.index) != gpu.info:
                self.client.map_set(
                    f"{GPU_INFO_PREFIX}{subject}",
                    json.dumps(gpu.info).encode("utf-8"),
                )
                self._published[gpu.index] = gpu.info

            for name, value in gpu.values.items():
                self.client.time_series_append(
                    f"{GPU_SERIES[name]}{subject}", float(value), stamp
                )


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
    hostname: str,
    interval: float = DEFAULT_MONITOR_INTERVAL_S,
    logger: logging.Logger | None = None,
) -> Monitor:
    """Start sampling this job's cgroup on this node, and return the running thread.

    `hostname` must name this node.
    The subject is `<job-id>:<hostname>`.
    `interval` is the time between readings, in seconds.
    """
    monitor = Monitor(
        client=client,
        subject=job_subject(slurm_job_id, hostname),
        prefixes=JOB_SERIES,
        sampler=CgroupSampler().sample,
        interval=interval,
        logger=logger,
    )
    monitor.start()
    return monitor


def start_gpu_monitor(
    client: DsServiceClient,
    slurm_job_id: str | int,
    hostname: str,
    interval: float = DEFAULT_MONITOR_INTERVAL_S,
    logger: logging.Logger | None = None,
) -> GpuMonitor | None:
    """Start sampling the GPUs this job can see on this node, and return the thread.

    Returns None, and starts nothing, where NVML finds no GPU,
    or cannot start, as on a node with no NVIDIA driver.
    `hostname` must name this node.
    `interval` is the time between readings, in seconds.
    """
    try:
        with nvml_session():
            found = pynvml.nvmlDeviceGetCount()
    except pynvml.NVMLError:
        return None
    if not found:
        return None

    monitor = GpuMonitor(
        client=client,
        subject=job_subject(slurm_job_id, hostname),
        interval=interval,
        logger=logger,
    )
    monitor.start()
    return monitor
