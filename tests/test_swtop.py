"""Tests for the `swtop` monitor.

The server is real, as everywhere else here,
so what the collector reports is what a live queue would tell it.
Slurm is mocked, since a worker's identity comes from the store
rather than from a running job.

The collector is async and these tests are not.
Each is given a collector on an event loop that lasts the whole test
(`LoopBound`) and calls `snapshot()` as if it were an ordinary method,
so a test can change the store between two polls
--- which is what the caching and staleness tests are about ---
without every test being written as a coroutine.
"""

from __future__ import annotations

import json
import asyncio
from typing import Any, Callable, Coroutine, TypeVar, cast
from datetime import datetime, timedelta, timezone

import pytest
from click.testing import CliRunner
from ds_service_client import DsServiceClientAsync

from slurm_workflows import swtop as swtop_mod
from slurm_workflows.swtop import UNNAMED, Collector, Snapshot, render, swtop
from worker_harness import make_worker
from test_monitors import wait_for

T = TypeVar("T")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def square(x):
    return x * x


class LoopBound:
    """A collector, and the one event loop its client belongs to.

    `DsServiceClientAsync` binds its channel to the loop running when it is made,
    so a fresh `asyncio.run` per call
    would leave the second poll talking to a loop that has closed.
    One loop is kept for the test instead.
    """

    def __init__(
        self,
        address: str,
        wrap: Callable[[DsServiceClientAsync], Any] = lambda client: client,
    ) -> None:
        self.loop = asyncio.new_event_loop()
        self.client = self.run(_make_client(address))
        self.collector = Collector(wrap(self.client), address)

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        return self.loop.run_until_complete(coro)

    def snapshot(self) -> Snapshot:
        return self.run(self.collector.snapshot())

    def close(self) -> None:
        self.run(self.client.close())
        self.loop.close()


async def _make_client(address: str) -> DsServiceClientAsync:
    """Build the client with the loop that will own it running."""
    return DsServiceClientAsync(address)


@pytest.fixture
def collector(ds_service_address):
    bound = LoopBound(ds_service_address)
    yield bound
    bound.close()


class CountingClient:
    """Records key reads, so the identity cache can be checked.

    Keyed by prefix, because a poll reads the progress display every time
    on top of the identities it is caching.
    """

    def __init__(self, inner):
        self._inner = inner
        self.keys_read: list[str] = []

    def reads(self, prefix: str) -> int:
        return sum(1 for key in self.keys_read if key.startswith(prefix))

    async def map_get(self, key):
        self.keys_read.append(key)
        return await self._inner.map_get(key)

    def __getattr__(self, name):
        return getattr(self._inner, name)


# --------------------------------------------------------------------------
# collecting
# --------------------------------------------------------------------------


class TestCollectTasks:
    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs):
        pilot_jobs("cpu")

    def test_an_idle_server_reports_nothing(self, collector):
        snapshot = collector.snapshot()

        assert snapshot.counts == {
            "ready": 0,
            "running": 0,
            "complete": 0,
            "canceled": 0,
        }
        assert snapshot.workers == []
        assert snapshot.tasks == []

    def test_every_task_is_listed_named_or_not(self, collector, executor):
        tasks = [executor.submit("cpu", square, i) for i in range(3)]

        snapshot = collector.snapshot()

        assert snapshot.counts["ready"] == 3
        assert [t.task_id for t in snapshot.tasks] == [t.task_id for t in tasks]
        assert [t.name for t in snapshot.tasks] == [UNNAMED] * 3

    def test_a_named_task_is_listed_under_its_name(self, collector, executor):
        task = executor.submit("cpu", square, 1)
        other = executor.submit("cpu", square, 2)
        executor.set_task_name(task, "the-named-one")

        listed = {t.task_id: t for t in collector.snapshot().tasks}

        assert listed[task.task_id].name == "the-named-one"
        assert listed[task.task_id].state == "Ready"
        assert listed[task.task_id].worker == ""
        assert listed[other.task_id].name == UNNAMED

    def test_a_name_published_after_a_poll_is_picked_up(self, collector, executor):
        task = executor.submit("cpu", square, 1)

        (before,) = collector.snapshot().tasks
        executor.set_task_name(task, "named-late")
        (after,) = collector.snapshot().tasks

        assert before.name == UNNAMED, "nothing had named it yet"
        assert after.name == "named-late", "the missing name is looked for again"

    def test_a_running_task_names_its_worker(
        self, collector, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", square, 1)
        executor.set_task_name(task, "in-flight")

        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")
        worker.client.task_get(worker.worker_id, "cpu")

        (listed,) = collector.snapshot().tasks

        assert listed.state == "Running"
        assert listed.worker == "w-1"
        worker.close()

    def test_an_unregistered_holder_is_shown_by_its_id(
        self, collector, executor, ds_client
    ):
        task = executor.submit("cpu", square, 1)
        executor.set_task_name(task, "in-flight")

        # Nothing published an identity for this one.
        ds_client.task_get("a-stranger", "cpu")

        (listed,) = collector.snapshot().tasks

        assert listed.worker == "a-stranger"

    def test_running_tasks_are_listed_first(
        self, collector, executor, ds_client, ds_service_address, tmp_path
    ):
        ready = executor.submit("cpu", square, 1)
        running = executor.submit("cpu", square, 2)
        executor.set_task_name(ready, "b-ready")
        executor.set_task_name(running, "a-running")

        # The queue is oldest first, so this claims `ready` --
        # claim both and let the second one stay Running.
        ds_client.task_get("stranger", "cpu")
        ds_client.task_get("stranger", "cpu")
        ds_client.task_done(ready.task_id, "stranger", b"")

        states = [(t.name, t.state) for t in collector.snapshot().tasks]

        assert states == [("a-running", "Running"), ("b-ready", "Complete")]


class TestCollectProgress:
    """The progress display a wait publishes, as the collector reads it."""

    def publish(
        self, ds_client, progress_id="p-1", desc="explore", unit="point", total=10
    ):
        ds_client.map_set(
            "progress_display",
            json.dumps(
                {
                    "progress_id": progress_id,
                    "desc": desc,
                    "unit": unit,
                    "total": total,
                }
            ).encode(),
        )

    def test_nothing_is_shown_on_an_idle_server(self, collector):
        assert collector.snapshot().progress is None

    def test_a_published_display_is_read(self, collector, ds_client):
        self.publish(ds_client, desc="squaring", unit="square", total=40)

        progress = collector.snapshot().progress

        assert progress is not None
        assert (progress.desc, progress.unit, progress.total) == (
            "squaring",
            "square",
            40,
        )
        assert progress.completed == 0

    def test_the_count_comes_from_the_series(self, collector, ds_client):
        self.publish(ds_client, progress_id="p-2", total=10)
        ds_client.time_series_append("progress:p-2", 4.0, _now_utc())

        progress = collector.snapshot().progress

        assert progress is not None
        assert progress.completed == 4
        assert progress.fraction == 0.4
        assert not progress.done

    def test_the_latest_point_wins(self, collector, ds_client):
        self.publish(ds_client, progress_id="p-3", total=10)
        for value in (0.0, 3.0, 10.0):
            ds_client.time_series_append("progress:p-3", value, _now_utc())

        progress = collector.snapshot().progress

        assert progress is not None
        assert progress.completed == 10
        assert progress.done

    def test_a_display_that_stopped_moving_keeps_its_count(self, collector, ds_client):
        """A finished wait writes nothing more; the last count still shows."""
        self.publish(ds_client, progress_id="p-4", total=10)
        ds_client.time_series_append("progress:p-4", 10.0, _now_utc())
        collector.snapshot()

        stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ds_client.time_series_append("progress:p-4", 10.0, stale)
        progress = collector.snapshot().progress

        assert progress is not None
        assert progress.completed == 10

    def test_an_unreadable_display_is_ignored(self, collector, ds_client):
        ds_client.map_set("progress_display", b"not json")

        assert collector.snapshot().progress is None


class TestCollectWorkerJobs:
    """The pilot jobs, as the executor left them in the store."""

    def test_a_job_appears_when_it_is_submitted(self, collector, executor):
        assert collector.snapshot().worker_jobs == []

        executor.define_worker("cpu", [])
        executor.scale_workers("cpu", 1)

        (listed,) = collector.snapshot().worker_jobs
        (worker_name,) = executor.groups["cpu"].workers
        assert listed.name == worker_name
        assert listed.group == "cpu"
        assert listed.slurm_job_id == str(
            executor.groups["cpu"].workers[worker_name].job_id
        )
        assert listed.submit_time

    def test_jobs_are_sorted_by_group_then_name(self, collector, executor):
        executor.define_worker("gpu", [])
        executor.define_worker("cpu", [])
        executor.scale_workers("gpu", 1)
        executor.scale_workers("cpu", 2)

        listed = collector.snapshot().worker_jobs

        assert [job.group for job in listed] == ["cpu", "cpu", "gpu"]
        assert listed[0].name < listed[1].name

    def test_a_job_with_no_process_is_still_listed(self, collector, executor):
        """Which is what a queued job looks like: submitted, not yet running."""
        executor.define_worker("cpu", [])
        executor.scale_workers("cpu", 1)

        snapshot = collector.snapshot()

        assert len(snapshot.worker_jobs) == 1
        assert snapshot.workers == []

    def test_a_description_that_cannot_be_read_is_shown_as_unknown(
        self, collector, ds_client
    ):
        ds_client.map_set("worker_job_info:half-written", b"not json")

        (listed,) = collector.snapshot().worker_jobs

        assert listed.name == "half-written"
        assert listed.slurm_job_id == "?"

    def test_a_job_is_read_once(self, ds_service_address, executor):
        """Written once when the job is submitted, so never read twice."""
        executor.define_worker("cpu", [])
        executor.scale_workers("cpu", 1)
        bound = LoopBound(ds_service_address, wrap=CountingClient)
        counting = cast(CountingClient, bound.collector.client)

        bound.snapshot()
        after_first = counting.reads("worker_job_info:")
        bound.snapshot()

        assert after_first == 1, "the whole description is one key"
        assert counting.reads("worker_job_info:") == after_first
        bound.close()


class TestCollectWorkers:
    def test_a_worker_appears_once_it_registers(
        self, collector, ds_service_address, tmp_path
    ):
        assert collector.snapshot().workers == []

        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        (listed,) = collector.snapshot().workers
        assert listed.worker_id == worker.worker_id
        assert listed.name == "w-1"
        assert listed.group == "cpu"
        assert listed.hostname == "testhost"
        assert listed.slurm_job_id == "42"
        assert listed.pid == "4242"
        worker.close()

    def test_workers_are_ordered_by_group_then_name(
        self, collector, ds_service_address, tmp_path
    ):
        # Named as the executor names them, because the worker id is built
        # from the name: two workers of one job and pid
        # are told apart by their names alone.
        workers = [
            make_worker(
                ds_service_address, tmp_path, group="gpu", name="run.worker.gpu.0"
            ),
            make_worker(
                ds_service_address, tmp_path, group="cpu", name="run.worker.cpu.1"
            ),
            make_worker(
                ds_service_address, tmp_path, group="cpu", name="run.worker.cpu.0"
            ),
        ]

        listed = [(w.group, w.name) for w in collector.snapshot().workers]

        assert listed == [
            ("cpu", "run.worker.cpu.0"),
            ("cpu", "run.worker.cpu.1"),
            ("gpu", "run.worker.gpu.0"),
        ]
        for worker in workers:
            worker.close()

    def test_an_identity_is_read_once_however_long_it_runs(
        self, ds_service_address, tmp_path
    ):
        """The published description never changes, so re-reading it is waste."""
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")
        # `CountingClient` forwards everything it does not count,
        # as the worker harness's doubles do,
        # so it stands in for a client without subclassing one.
        bound = LoopBound(ds_service_address, wrap=CountingClient)
        counting = cast(CountingClient, bound.collector.client)

        bound.snapshot()
        after_first = counting.reads("worker_process_info:")
        bound.snapshot()

        assert after_first == 1, "the whole description is one key"
        assert counting.reads("worker_process_info:") == after_first
        worker.close()
        bound.close()

    def test_a_description_that_cannot_be_read_is_shown_as_unknown(
        self, collector, ds_client
    ):
        ds_client.map_set("worker_process_info:something-else", b"not json")

        (listed,) = collector.snapshot().workers

        assert listed.worker_id == "something-else"
        assert listed.name == "?"

    def test_an_unreadable_description_is_not_cached(self, collector, ds_client):
        """It may be a writer this reader arrived in the middle of."""
        ds_client.map_set("worker_process_info:w", b"not json")
        collector.snapshot()

        ds_client.map_set(
            "worker_process_info:w",
            json.dumps(
                {
                    "group": "cpu",
                    "name": "w-1",
                    "slurm_job_id": 42,
                    "hostname": "testhost",
                    "pid": 1,
                }
            ).encode(),
        )

        (listed,) = collector.snapshot().workers
        assert listed.name == "w-1"


class TestCollectMonitored:
    """Host and job readings, as the monitors leave them in the store."""

    def test_nothing_is_monitored_on_an_idle_server(self, collector):
        snapshot = collector.snapshot()

        assert snapshot.hosts == []
        assert snapshot.jobs == []

    def test_a_monitored_host_and_job_appear(
        self, collector, ds_client, ds_service_address, tmp_path
    ):
        worker = make_worker(ds_service_address, tmp_path)
        assert wait_for(
            lambda: bool(ds_client.time_series_get("host_free_memory:testhost"))
        )

        snapshot = collector.snapshot()

        (host,) = snapshot.hosts
        assert host.subject == "testhost"
        assert host.values["free_memory"] > 0
        assert not host.stale

        (job,) = snapshot.jobs
        assert job.subject == "42"
        assert job.values["memory"] > 0
        worker.close()

    def test_the_latest_point_is_the_one_shown(self, collector, ds_client):
        for value in [1.0, 2.0, 3.0]:
            ds_client.time_series_append("host_load_average:node-1", value, _now_utc())
        ds_client.time_series_append("host_free_memory:node-1", 5.0, _now_utc())

        (host,) = collector.snapshot().hosts

        assert host.values["load_average"] == 3.0

    def test_a_subject_with_only_old_points_is_stale(self, collector, ds_client):
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ds_client.time_series_append("host_free_memory:node-1", 5.0, old)

        (host,) = collector.snapshot().hosts

        assert host.subject == "node-1"
        assert host.values == {}
        assert host.stale

    def test_a_series_that_never_started_leaves_its_column_out(
        self, collector, ds_client
    ):
        """Only one of a host's four series has to exist for it to be listed."""
        ds_client.time_series_append("host_free_memory:node-1", 5.0, _now_utc())

        (host,) = collector.snapshot().hosts

        assert set(host.values) == {"free_memory"}


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------


class TestRender:
    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs):
        pilot_jobs("cpu")

    def test_a_progress_display_is_drawn_as_a_bar(self, collector, ds_client):
        ds_client.map_set(
            "progress_display",
            json.dumps(
                {
                    "progress_id": "p-9",
                    "desc": "squaring",
                    "unit": "square",
                    "total": 8,
                }
            ).encode(),
        )
        ds_client.time_series_append("progress:p-9", 2.0, _now_utc())

        out = render(collector.snapshot())

        assert "squaring" in out
        assert "2/8 square" in out
        assert "25%" in out
        assert "#" in out and "-" in out

    def test_no_progress_line_without_a_display(self, collector):
        assert "%" not in render(Snapshot(address="a", when=datetime.now()))

    def test_an_idle_server_says_so(self, collector):
        """A pilot job is submitted here, but nothing is running in it yet."""
        out = render(collector.snapshot())

        assert "total 0" in out
        assert "worker jobs (1)" in out
        assert "no worker processes have registered" in out
        assert "no tasks have been submitted" in out

    def test_a_server_with_nothing_submitted_says_so(self):
        out = render(Snapshot(address="a", when=datetime.now()))

        assert "no pilot jobs have been submitted" in out
        assert "no worker processes have registered" in out

    def test_the_tables_carry_the_data(
        self, collector, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", square, 1)
        executor.set_task_name(task, "the-named-one")
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        out = render(collector.snapshot())

        assert "worker processes (1)" in out
        assert "tasks (1)" in out
        for expected in ["w-1", "testhost", "4242", "the-named-one", task.task_id]:
            assert expected in out
        worker.close()

    def test_an_unmonitored_server_says_so(self, collector):
        out = render(collector.snapshot())

        assert "no host is being monitored" in out
        assert "no slurm job is being monitored" in out

    def test_the_readings_are_shown_in_readable_units(self, collector, ds_client):
        for key, value in [
            ("host_free_memory:node-1", 2 * 1024**3),
            ("host_load_average:node-1", 3.5),
            ("host_dev_shm_used:node-1", 12.25),
            ("host_tmp_used:node-1", 46.9),
            ("slurm_job_memory:1846231", 1024**3),
            ("slurm_job_cpu:1846231", 12.4),
        ]:
            ds_client.time_series_append(key, value, _now_utc())

        out = render(collector.snapshot())

        assert "2.0G" in out
        assert "3.50" in out
        assert "12.2%" in out
        assert "1.0G" in out
        assert "12.4 cores" in out

    def test_a_stale_subject_is_labelled_not_dropped(self, collector, ds_client):
        """A monitor that died is worth seeing, not hiding."""
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        ds_client.time_series_append("host_free_memory:node-1", 5.0, old)

        out = render(collector.snapshot())

        assert "node-1 (stale)" in out
        assert "hosts (1)" in out

    def test_a_missing_measurement_is_a_dash(self, collector, ds_client):
        # A subject name without a dash of its own,
        # so the dashes asserted below can only be the empty columns.
        ds_client.time_series_append("host_free_memory:nodeone", 5.0, _now_utc())

        out = render(collector.snapshot())

        row = next(line for line in out.splitlines() if line.startswith("nodeone"))
        assert row.split() == ["nodeone", "5.0B", "-", "-", "-"]

    def test_a_failed_poll_is_reported_in_place_of_the_tables(self, collector):
        snapshot = collector.snapshot()
        snapshot.error = "TimeoutError: server unreachable"

        out = render(snapshot)

        assert "cannot read the server: TimeoutError" in out
        assert "workers" not in out

    def test_every_line_fits_together(self, collector):
        """Columns are padded, so no row may be ragged or unterminated."""
        out = render(collector.snapshot())

        assert out.endswith("\n")
        assert not any(line.endswith(" ") for line in out.splitlines())


# --------------------------------------------------------------------------
# the command line
# --------------------------------------------------------------------------


class TestCli:
    @pytest.fixture
    def stop_after_one_poll(self, monkeypatch):
        """Let one frame be drawn, then interrupt as a user would."""

        async def sleep(seconds):
            raise KeyboardInterrupt

        monkeypatch.setattr(swtop_mod.asyncio, "sleep", sleep)

    def test_it_polls_and_stops_on_interrupt(
        self, ds_service_address, stop_after_one_poll
    ):
        result = CliRunner().invoke(swtop, [ds_service_address])

        assert result.exit_code == 0
        assert "swtop" in result.output
        assert "total 0" in result.output

    def test_the_interval_defaults_to_two_seconds(
        self, ds_service_address, monkeypatch
    ):
        slept = []

        async def sleep(seconds):
            slept.append(seconds)
            raise KeyboardInterrupt

        monkeypatch.setattr(swtop_mod.asyncio, "sleep", sleep)

        CliRunner().invoke(swtop, [ds_service_address], catch_exceptions=False)

        assert slept == [2.0]

    def test_the_interval_can_be_set(self, ds_service_address, monkeypatch):
        slept = []

        async def sleep(seconds):
            slept.append(seconds)
            raise KeyboardInterrupt

        monkeypatch.setattr(swtop_mod.asyncio, "sleep", sleep)

        CliRunner().invoke(swtop, [ds_service_address, "-i", "0.5"])

        assert slept == [0.5]

    @pytest.mark.parametrize("interval", ["0", "-1"])
    def test_a_non_positive_interval_is_rejected(self, ds_service_address, interval):
        result = CliRunner().invoke(swtop, [ds_service_address, "-i", interval])

        assert result.exit_code != 0

    def test_the_address_is_required(self):
        result = CliRunner().invoke(swtop, [])

        assert result.exit_code != 0

    def test_an_unreachable_server_is_reported_not_fatal(self, stop_after_one_poll):
        """A monitor that quits when the server blinks is not much of a monitor."""
        result = CliRunner().invoke(swtop, ["127.0.0.1:1"])

        assert result.exit_code == 0
        assert "cannot read the server" in result.output


def test_a_snapshot_needs_only_an_address_and_a_time():
    """The error path builds one without ever reaching the server."""
    snapshot = Snapshot(address="host:1", when=swtop_mod.datetime.now())

    assert snapshot.counts == {}
    assert snapshot.workers == []
    assert snapshot.tasks == []
