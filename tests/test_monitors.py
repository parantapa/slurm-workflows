"""Tests for the host, Slurm job and GPU monitors."""

# The samplers read this machine,
# so the assertions are about shape and plausibility
# rather than exact numbers.
# The tests point the cgroup reader at files they write themselves,
# which is the only way to assert on values a kernel decides.
# The GPU tests read the fake NVML in `conftest.py` instead.

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

import psutil
import pytest

from slurm_workflows import monitors as monitors_mod
from slurm_workflows.monitors import (
    GPU_INFO_PREFIX,
    GPU_SERIES,
    HOST_SERIES,
    JOB_SERIES,
    CgroupSampler,
    GpuMonitor,
    GpuReading,
    Monitor,
    gpu_subject,
    job_subject,
    nvml_session,
    query_gpus,
    sample_host,
    split_gpu_subject,
    split_job_subject,
    start_gpu_monitor,
    start_host_monitor,
    start_slurm_job_monitor,
)

import pynvml

from conftest import FakeGpu, FakeNvml

GIB = 1024**3


def two_gpus(fake_nvml: FakeNvml) -> None:
    """Give the fake NVML two GPUs, the second one idle."""
    fake_nvml.gpus = [
        FakeGpu(uuid="GPU-aaaa", memory_used=GIB, utilization=45),
        FakeGpu(uuid="GPU-bbbb", memory_used=0, utilization=0),
    ]


def wait_for(predicate: Callable[[], object], timeout: float = 5.0) -> bool:
    """Poll `predicate` until it holds, or the timeout runs out."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class TestSampleHost:
    def test_it_reports_memory_load_and_scratch(self):
        values = sample_host()

        assert values["free_memory"] > 0
        assert values["load_average"] >= 0
        for name in ["dev_shm_used", "tmp_used"]:
            assert 0 <= values[name] <= 100

    def test_an_unmounted_filesystem_is_left_out(self, monkeypatch):
        """Absent is not the same as empty, so it gets no value at all."""

        # Kept before patching:
        # `monitors_mod.psutil` is the psutil module itself,
        # so the patch otherwise replaces what `disk_usage` calls.
        real_disk_usage = psutil.disk_usage

        def disk_usage(path):
            if path == "/dev/shm":
                raise OSError("not mounted")
            return real_disk_usage(path)

        monkeypatch.setattr(monitors_mod.psutil, "disk_usage", disk_usage)

        values = sample_host()

        assert "dev_shm_used" not in values
        assert "tmp_used" in values


class TestCgroupSampler:
    @staticmethod
    def write_cgroup(root: Path, memory: int, cpu_usec: int) -> None:
        (root / "memory.current").write_text(f"{memory}\n")
        (root / "cpu.stat").write_text(
            f"usage_usec {cpu_usec}\nuser_usec {cpu_usec}\nsystem_usec 0\n"
        )

    def test_it_reads_the_cgroups_own_accounting(self, tmp_path):
        self.write_cgroup(tmp_path, memory=4096, cpu_usec=1_000_000)

        values = CgroupSampler(tmp_path).sample()

        assert values["memory"] == 4096
        assert values["cpu"] == 0.0, "the first sample has nothing to difference"

    def test_cpu_is_the_rate_between_two_samples(self, tmp_path):
        sampler = CgroupSampler(tmp_path)
        self.write_cgroup(tmp_path, memory=4096, cpu_usec=0)
        sampler.sample()

        # Two cores' worth of CPU seconds over the elapsed wall clock.
        time.sleep(0.05)
        elapsed = 0.05
        self.write_cgroup(tmp_path, memory=4096, cpu_usec=int(2 * elapsed * 1e6))
        values = sampler.sample()

        # `sleep` can overrun, so the measured interval is longer
        # and the rate lower than 2.
        # The bound asks only that a busy cgroup reports cores in use.
        assert values["cpu"] > 0.5, "a busy cgroup reports cores in use"

    def test_a_counter_that_restarts_reports_no_time(self, tmp_path):
        """A recreated cgroup starts from zero. That is not negative CPU."""
        sampler = CgroupSampler(tmp_path)
        self.write_cgroup(tmp_path, memory=4096, cpu_usec=10_000_000)
        sampler.sample()

        self.write_cgroup(tmp_path, memory=4096, cpu_usec=0)
        values = sampler.sample()

        assert values["cpu"] == 0.0

    def test_it_falls_back_to_counting_processes(self, tmp_path):
        """No cgroup files: a login node, cgroup v1, or a plain container."""
        values = CgroupSampler(tmp_path).sample()

        assert values["memory"] > 0
        assert values["cpu"] == 0.0

    def test_a_malformed_cpu_stat_falls_back_too(self, tmp_path):
        (tmp_path / "memory.current").write_text("4096\n")
        (tmp_path / "cpu.stat").write_text("nr_periods 0\n")

        values = CgroupSampler(tmp_path).sample()

        assert values["memory"] > 0
        assert values["memory"] != 4096, "the partial cgroup reading is not used"

    def test_a_cgroup_naming_no_readable_process_falls_back(self, tmp_path):
        """The root cgroup of a systemd host names only kernel threads.

        A kernel thread has no address space, so the sum over them is zero.
        """
        # A pid well above every live one, so it names no process.
        dead = max(psutil.pids()) + 1000
        assert not psutil.pid_exists(dead)
        (tmp_path / "cgroup.procs").write_text(f"{dead}\n")

        values = CgroupSampler(tmp_path).sample()

        assert values["memory"] > 0, "the sum falls back to this process's tree"

    def test_the_real_cgroup_is_readable_or_falls_back(self):
        values = CgroupSampler().sample()

        assert values["memory"] > 0
        assert values["cpu"] == 0.0


class TestMonitor:
    def test_one_reading_lands_in_the_subjects_series(
        self, ds_client, ds_service_address
    ):
        monitor = Monitor(
            client=ds_client,
            subject="node-1",
            prefixes=HOST_SERIES,
            sampler=lambda: {"free_memory": 17.0, "load_average": 0.5},
        )

        monitor.append_sample()

        (point,) = ds_client.time_series_get("host_free_memory:node-1")
        assert point.value == 17.0
        assert ds_client.time_series_get("host_load_average:node-1")[-1].value == 0.5

    def test_the_readings_of_one_sample_share_a_timestamp(self, ds_client):
        monitor = Monitor(
            client=ds_client,
            subject="node-1",
            prefixes=HOST_SERIES,
            sampler=lambda: {"free_memory": 1.0, "load_average": 2.0},
        )

        monitor.append_sample()

        memory = ds_client.time_series_get("host_free_memory:node-1")[-1]
        load = ds_client.time_series_get("host_load_average:node-1")[-1]
        assert memory.datetime == load.datetime

    def test_it_keeps_sampling_until_stopped(self, ds_client):
        monitor = Monitor(
            client=ds_client,
            subject="node-1",
            prefixes=HOST_SERIES,
            sampler=lambda: {"load_average": 1.0},
            interval=0.01,
        )
        monitor.start()

        assert wait_for(
            lambda: len(ds_client.time_series_get("host_load_average:node-1")) >= 3
        )
        monitor.stop()

        assert not monitor.is_alive()

    def test_a_sampler_that_raises_does_not_end_it(self, ds_client, caplog):
        """A node that is briefly unreachable must not stop the series."""

        def boom():
            raise OSError("no /proc today")

        monitor = Monitor(
            client=ds_client,
            subject="node-1",
            prefixes=HOST_SERIES,
            sampler=boom,
            interval=0.01,
        )
        monitor.start()

        assert wait_for(lambda: "failed to sample" in caplog.text.lower())
        assert monitor.is_alive()
        monitor.stop()

    def test_stopping_one_that_never_ran_is_fine(self, ds_client):
        monitor = Monitor(
            client=ds_client,
            subject="node-1",
            prefixes=HOST_SERIES,
            sampler=sample_host,
        )

        monitor.stop()  # must not raise

        assert not monitor.is_alive()


class TestStartHelpers:
    def test_the_host_monitor_writes_the_host_series(
        self, ds_client, ds_service_address
    ):
        monitor = start_host_monitor(ds_client, "node-1", interval=60.0)

        assert wait_for(
            lambda: bool(ds_client.time_series_get("host_free_memory:node-1"))
        )
        monitor.stop()

    def test_the_job_monitor_writes_the_job_series_of_its_node(self, ds_client):
        monitor = start_slurm_job_monitor(ds_client, 12345, "node-1", interval=60.0)

        assert wait_for(
            lambda: bool(ds_client.time_series_get("slurm_job_memory:12345:node-1"))
        )
        assert ds_client.time_series_get("slurm_job_cpu:12345:node-1")[-1].value == 0.0
        monitor.stop()

    def test_a_numeric_job_id_keys_the_series_as_text(self, ds_client):
        monitor = start_slurm_job_monitor(ds_client, 7, "node-1", interval=60.0)
        monitor.stop()

        assert ds_client.time_series_search_key("^slurm_job_memory:") == [
            "slurm_job_memory:7:node-1"
        ]

    def test_two_nodes_of_one_job_write_series_of_their_own(self, ds_client):
        first = start_slurm_job_monitor(ds_client, 7, "node-1", interval=60.0)
        second = start_slurm_job_monitor(ds_client, 7, "node-2", interval=60.0)
        first.stop()
        second.stop()

        assert sorted(ds_client.time_series_search_key("^slurm_job_memory:")) == [
            "slurm_job_memory:7:node-1",
            "slurm_job_memory:7:node-2",
        ]


class TestJobSubject:
    def test_a_subject_splits_back_into_its_job_and_its_node(self):
        assert split_job_subject(job_subject(1846231, "udc-an28")) == (
            "1846231",
            "udc-an28",
        )

    def test_a_subject_without_a_node_has_an_empty_hostname(self):
        assert split_job_subject("1846231") == ("1846231", "")


class TestGpuSubject:
    def test_a_subject_splits_back_into_its_job_node_and_gpu(self):
        assert split_gpu_subject(gpu_subject(1846231, "udc-an28", 3)) == (
            "1846231",
            "udc-an28",
            "3",
        )

    def test_it_extends_the_subject_of_its_job(self):
        assert gpu_subject(7, "node-1", 0) == f"{job_subject(7, 'node-1')}:0"


class TestNvmlSession:
    def test_it_shuts_down_what_it_started(self, fake_nvml):
        with nvml_session():
            assert fake_nvml.sessions == 1

        assert fake_nvml.sessions == 0

    def test_no_driver_raises(self, fake_nvml):
        fake_nvml.no_driver = True

        with pytest.raises(pynvml.NVMLError) as raised:
            with nvml_session():
                pass

        assert getattr(raised.value, "value") == pynvml.NVML_ERROR_LIBRARY_NOT_FOUND


class TestQueryGpus:
    def test_it_reads_each_gpu(self, fake_nvml):
        two_gpus(fake_nvml)

        with nvml_session():
            first, second = query_gpus()

        assert (first.index, first.uuid, first.name) == (
            "0",
            "GPU-aaaa",
            "NVIDIA A100-SXM4-80GB",
        )
        assert first.memory_total == 80 * GIB
        assert first.values == {
            "memory_used": 1 * GIB,
            "memory_free": 79 * GIB,
            "utilization": 45.0,
        }
        assert (second.index, second.uuid) == ("1", "GPU-bbbb")

    def test_an_unsupported_measurement_is_left_out(self, fake_nvml):
        # A MIG instance reports no utilization.
        fake_nvml.gpus = [FakeGpu(utilization=None)]

        with nvml_session():
            (gpu,) = query_gpus()

        assert set(gpu.values) == {"memory_used", "memory_free"}

    def test_unsupported_memory_leaves_out_the_total_too(self, fake_nvml):
        fake_nvml.gpus = [FakeGpu(memory_total=None)]

        with nvml_session():
            (gpu,) = query_gpus()

        assert gpu.memory_total is None
        assert set(gpu.values) == {"utilization"}

    def test_no_gpu_is_an_empty_list(self, fake_nvml):
        with nvml_session():
            assert query_gpus() == []

    def test_outside_a_session_it_raises(self, fake_nvml):
        two_gpus(fake_nvml)

        with pytest.raises(pynvml.NVMLError) as raised:
            query_gpus()

        assert getattr(raised.value, "value") == pynvml.NVML_ERROR_UNINITIALIZED


class TestGpuMonitor:
    def test_each_gpu_gets_series_of_its_own(self, ds_client, fake_nvml):
        two_gpus(fake_nvml)
        monitor = GpuMonitor(ds_client, "7:node-1")

        with nvml_session():
            monitor.append_sample()

        for index in ["0", "1"]:
            for name, prefix in GPU_SERIES.items():
                points = ds_client.time_series_get(f"{prefix}7:node-1:{index}")
                assert len(points) == 1, name
        used = ds_client.time_series_get("slurm_job_gpu_memory_used:7:node-1:0")
        assert used[-1].value == 1 * GIB

    def test_the_type_of_each_gpu_goes_in_the_map(self, ds_client, fake_nvml):
        two_gpus(fake_nvml)
        monitor = GpuMonitor(ds_client, "7:node-1")

        with nvml_session():
            monitor.append_sample()

        info = json.loads(ds_client.map_get(f"{GPU_INFO_PREFIX}7:node-1:0"))
        assert info == {
            "name": "NVIDIA A100-SXM4-80GB",
            "uuid": "GPU-aaaa",
            "memory_total": 80 * GIB,
        }

    def test_the_type_is_written_again_only_when_it_changes(self, ds_client, fake_nvml):
        fake_nvml.gpus = [FakeGpu()]
        monitor = GpuMonitor(ds_client, "7:node-1")
        key = f"{GPU_INFO_PREFIX}7:node-1:0"

        with nvml_session():
            monitor.append_sample()
            ds_client.map_set(key, b"overwritten by the test")
            monitor.append_sample()
            assert ds_client.map_get(key) == b"overwritten by the test"

            fake_nvml.gpus = [FakeGpu(name="NVIDIA H100")]
            monitor.append_sample()

        assert json.loads(ds_client.map_get(key))["name"] == "NVIDIA H100"

    def test_the_gpus_of_one_reading_share_a_timestamp(self, ds_client, fake_nvml):
        two_gpus(fake_nvml)
        monitor = GpuMonitor(ds_client, "7:node-1")

        with nvml_session():
            monitor.append_sample()

        stamps = {
            ds_client.time_series_get(f"{prefix}7:node-1:{index}")[-1].datetime
            for prefix in GPU_SERIES.values()
            for index in ["0", "1"]
        }
        assert len(stamps) == 1

    def test_its_thread_name_differs_from_the_job_monitors(self, ds_client):
        monitor = GpuMonitor(ds_client, "7:node-1")

        assert monitor.subject == "7:node-1"
        assert monitor.name == "gpu-monitor:7:node-1"

    def test_the_thread_holds_one_session_while_it_runs(self, ds_client, fake_nvml):
        fake_nvml.gpus = [FakeGpu()]
        monitor = GpuMonitor(ds_client, "7:node-1", interval=60.0)
        monitor.start()

        assert wait_for(
            lambda: bool(
                ds_client.time_series_get("slurm_job_gpu_utilization:7:node-1:0")
            )
        )
        assert fake_nvml.sessions == 1
        monitor.stop()
        assert fake_nvml.sessions == 0

    def test_a_thread_that_cannot_start_nvml_logs_it_and_ends(
        self, ds_client, fake_nvml, caplog
    ):
        fake_nvml.no_driver = True
        monitor = GpuMonitor(ds_client, "7:node-1", interval=60.0)

        monitor.start()
        monitor.join(5.0)

        assert not monitor.is_alive()
        assert "cannot use NVML" in caplog.text


class TestStartGpuMonitor:
    def test_it_samples_the_gpus_nvml_lists(self, ds_client, fake_nvml):
        two_gpus(fake_nvml)

        monitor = start_gpu_monitor(ds_client, 7, "node-1", interval=60.0)

        assert monitor is not None
        assert wait_for(
            lambda: bool(
                ds_client.time_series_get("slurm_job_gpu_utilization:7:node-1:1")
            )
        )
        monitor.stop()

    def test_a_node_with_no_gpu_starts_nothing(self, ds_client, fake_nvml):
        assert start_gpu_monitor(ds_client, 7, "node-1", interval=60.0) is None
        assert fake_nvml.sessions == 0

    def test_a_node_with_no_driver_starts_nothing(self, ds_client, fake_nvml):
        fake_nvml.no_driver = True

        assert start_gpu_monitor(ds_client, 7, "node-1", interval=60.0) is None


def test_the_series_maps_do_not_overlap():
    """A subject's prefix has to say which monitor wrote it."""
    maps = [HOST_SERIES, JOB_SERIES, GPU_SERIES]
    prefixes = [prefix for series in maps for prefix in series.values()]
    assert len(prefixes) == len(set(prefixes))


def test_no_job_series_prefix_starts_a_gpu_series_prefix():
    """`swtop` finds the subjects of a series by searching on its prefix."""
    for job_prefix in JOB_SERIES.values():
        for gpu_prefix in [*GPU_SERIES.values(), GPU_INFO_PREFIX]:
            assert not gpu_prefix.startswith(job_prefix)


@pytest.mark.parametrize("prefixes", [HOST_SERIES, JOB_SERIES, GPU_SERIES])
def test_every_series_prefix_ends_with_a_separator(prefixes):
    """The monitor appends the subject raw, so the prefix carries the colon."""
    assert all(prefix.endswith(":") for prefix in prefixes.values())
