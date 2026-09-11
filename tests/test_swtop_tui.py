"""Tests for the `swtop` terminal UI.

Textual is async and the tests around it are not:
each drives the app inside `asyncio.run`.
That keeps this file the same shape as the rest of the suite,
and needs no pytest plugin.

Each test drives the app headlessly through `run_test`,
and asserts on the state of the widgets rather than on pixels.
The poll runs in a Textual worker, so anything that waits for a poll
waits on `app.workers`, never on a sleep.

The scenario builds a collector against the real server,
rather than a fixture.
Its client belongs to the event loop that made it,
and `run_test` runs that loop.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from typing import cast

import pytest
from textual.app import App, ComposeResult
from textual.widgets import DataTable, ProgressBar, Static

from slurm_workflows.swtop import (
    Collector,
    Snapshot,
    SubjectInfo,
    ProgressInfo,
    TaskInfo,
    WorkerInfo,
    WorkerJobInfo,
    open_collector,
)
from slurm_workflows.swtop_tui import SwtopApp, sync_table
from worker_harness import make_worker


def square(x):
    return x * x


def text_of(widget: Static) -> str:
    """What a `Static` shows now."""
    return str(widget.content)


def table_of(app: SwtopApp, block: str) -> DataTable:
    return app.query_one(f"#{block}").query_one(DataTable)


def rows_of(app: SwtopApp, block: str) -> list[list[str]]:
    table = table_of(app, block)
    return [table.get_row(key) for key in table.rows]


def keys_of(app: SwtopApp, block: str) -> list[str]:
    return [str(key.value) for key in table_of(app, block).rows]


def snapshot(**kwargs) -> Snapshot:
    """A snapshot with everything filled in unless a test says otherwise."""
    filled = Snapshot(
        address="host:1",
        when=datetime.now(),
        counts={"ready": 1, "running": 2, "complete": 3, "canceled": 0},
        progress=ProgressInfo("p-1", "explore", "point", 8, 2),
        worker_jobs=[
            WorkerJobInfo("run.worker.cpu.0", "cpu", "42", "2026-09-07T11:04:57-04:00")
        ],
        workers=[WorkerInfo("w-id", "cpu", "run.worker.cpu.0", "42", "node-1", "17")],
        hosts=[
            SubjectInfo("node-1", {"free_memory": 2 * 1024**3, "load_average": 3.5})
        ],
        jobs=[SubjectInfo("42", {"memory": 1024**3, "cpu": 12.4})],
        tasks=[TaskInfo("run.task.0", "train-7", "Running", "run.worker.cpu.0")],
    )
    return replace(filled, **kwargs)


def drive(scenario):
    """Run one async scenario against a fresh app."""
    asyncio.run(scenario())


class StubCollector:
    """Hands back one snapshot, or fails, without a server behind it."""

    def __init__(self, snapshot: Snapshot | None = None, address: str = "host:1"):
        self.address = address
        self._snapshot = snapshot

    async def snapshot(self) -> Snapshot:
        if self._snapshot is None:
            raise TimeoutError("nothing to give")
        return self._snapshot


def as_collector(stub: StubCollector) -> Collector:
    """Type the stand-in as the collector it stands in for."""
    return cast(Collector, stub)


class TableApp(App):
    """One mounted `DataTable`, which is all `sync_table` needs."""

    def compose(self) -> ComposeResult:
        yield DataTable()

    def on_mount(self) -> None:
        self.query_one(DataTable).add_columns("A", "B")


def synced(*updates: list[tuple[str, list[str]]]) -> dict:
    """Apply each update to a mounted table, and report what is in it."""
    seen: dict = {}

    async def scenario():
        app = TableApp()
        async with app.run_test():
            table = app.query_one(DataTable)
            for rows in updates:
                sync_table(table, rows)
            seen["keys"] = [str(key.value) for key in table.rows]
            seen["rows"] = {str(key.value): table.get_row(key) for key in table.rows}
            seen["count"] = table.row_count

    drive(scenario)
    return seen


# --------------------------------------------------------------------------
# Updating a table in place
# --------------------------------------------------------------------------


class TestSyncTable:
    """What keeps a scroll position: an update to the rows, not a rebuild."""

    def test_it_adds_the_rows_it_is_given(self):
        seen = synced([("k1", ["a", "b"]), ("k2", ["c", "d"])])

        assert seen["count"] == 2
        assert seen["rows"]["k2"] == ["c", "d"]

    def test_a_row_that_is_still_there_keeps_its_place(self):
        seen = synced(
            [("k1", ["a", "b"]), ("k2", ["c", "d"])],
            [("k1", ["a", "b"]), ("k2", ["c", "d"])],
        )

        assert seen["keys"] == ["k1", "k2"]

    def test_a_changed_cell_is_updated(self):
        seen = synced([("k1", ["a", "b"])], [("k1", ["a", "CHANGED"])])

        assert seen["rows"]["k1"] == ["a", "CHANGED"]
        assert seen["count"] == 1

    def test_a_row_that_is_gone_is_removed(self):
        seen = synced([("k1", ["a", "b"]), ("k2", ["c", "d"])], [("k2", ["c", "d"])])

        assert seen["keys"] == ["k2"]

    def test_a_new_row_is_added_after_the_others(self):
        seen = synced([("k1", ["a", "b"])], [("k1", ["a", "b"]), ("k2", ["c", "d"])])

        assert seen["keys"] == ["k1", "k2"]

    def test_emptying_it_leaves_no_rows(self):
        seen = synced([("k1", ["a", "b"])], [])

        assert seen["count"] == 0


# --------------------------------------------------------------------------
# What the app draws
# --------------------------------------------------------------------------


class TestDisplay:
    def test_every_block_is_filled_in(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())

                assert rows_of(app, "worker-jobs") == [
                    ["run.worker.cpu.0", "cpu", "42", "2026-09-07T11:04:57-04:00"]
                ]
                assert rows_of(app, "workers") == [
                    ["run.worker.cpu.0", "cpu", "node-1", "42", "17"]
                ]
                assert rows_of(app, "hosts")[0][:3] == ["node-1", "2.0G", "3.50"]
                assert rows_of(app, "jobs") == [["42", "1.0G", "12.4 cores"]]
                assert rows_of(app, "tasks") == [
                    ["train-7", "run.task.0", "Running", "run.worker.cpu.0"]
                ]

        drive(scenario)

    def test_a_job_with_no_process_shows_in_one_block_only(self):
        """A queued pilot job: submitted, and nothing running in it yet."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot(workers=[]))

                assert len(rows_of(app, "worker-jobs")) == 1
                assert rows_of(app, "workers") == []
                empty = app.query_one("#workers").query_one(".block-empty", Static)
                assert "no worker processes have registered" in text_of(empty)

        drive(scenario)

    def test_the_progress_display_is_a_bar(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())

                block = app.query_one("#progress")
                assert block.display
                label = text_of(block.query_one(".progress-label", Static))
                assert "explore" in label and "2/8 point" in label
                bar = block.query_one(ProgressBar)
                assert (bar.total, bar.progress) == (8, 2)

        drive(scenario)

    def test_no_display_hides_the_bar(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot(progress=None))

                assert not app.query_one("#progress").display

        drive(scenario)

    def test_the_summary_carries_the_counts(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())

                summary = text_of(app.query_one("#summary", Static))
                assert "ready 1" in summary
                assert "total 6" in summary

        drive(scenario)

    def test_a_block_with_nothing_in_it_says_why(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot(hosts=[], workers=[], jobs=[], tasks=[]))

                block = app.query_one("#hosts")
                assert not block.query_one(DataTable).display
                empty = block.query_one(".block-empty", Static)
                assert empty.display
                assert "no host is being monitored" in text_of(empty)

        drive(scenario)

    def test_a_block_counts_its_rows_in_its_title(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(
                    snapshot(
                        workers=[
                            WorkerInfo(f"id-{i}", "cpu", f"w-{i}", "42", "node", "1")
                            for i in range(3)
                        ]
                    )
                )

                title = app.query_one("#workers").query_one(".block-title", Static)
                assert text_of(title) == "worker processes (3)"

        drive(scenario)

    def test_rows_keep_their_identity_across_updates(self):
        """The reason for keying: a scroll position survives a poll."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())
                first = keys_of(app, "workers")

                app.apply(snapshot())

                assert keys_of(app, "workers") == first

        drive(scenario)

    def test_a_worker_that_appears_is_added_to_the_table(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())

                more = [
                    WorkerInfo("w-id", "cpu", "run.worker.cpu.0", "42", "node-1", "17"),
                    WorkerInfo(
                        "w-id2", "cpu", "run.worker.cpu.1", "42", "node-2", "18"
                    ),
                ]
                app.apply(snapshot(workers=more))

                assert keys_of(app, "workers") == ["w-id", "w-id2"]

        drive(scenario)


class TestFailedPoll:
    def test_it_is_reported_on_the_screen(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot(error="TimeoutError: server unreachable"))

                error = app.query_one("#error", Static)
                assert error.display
                assert "cannot read the server" in text_of(error)

        drive(scenario)

    def test_the_last_good_reading_is_left_on_the_screen(self):
        """A server that restarts must not blank the display."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot())

                app.apply(snapshot(error="TimeoutError: server unreachable"))

                assert rows_of(app, "workers") == [
                    ["run.worker.cpu.0", "cpu", "node-1", "42", "17"]
                ]

        drive(scenario)

    def test_a_good_poll_clears_the_error(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.apply(snapshot(error="TimeoutError: server unreachable"))

                app.apply(snapshot())

                assert not app.query_one("#error", Static).display

        drive(scenario)


# --------------------------------------------------------------------------
# Against a real server
# --------------------------------------------------------------------------


class TestRealServer:
    """The polling half: a thread, a real client, and the widgets it fills."""

    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs):
        pilot_jobs("cpu")

    def test_it_polls_on_startup(self, executor, ds_service_address, tmp_path):
        task = executor.submit("cpu", square, 5)
        executor.set_task_name(task, "the-named-one")
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        async def scenario():
            async with open_collector(ds_service_address) as collector:
                app = SwtopApp(collector, 60.0)
                async with app.run_test() as pilot:
                    await app.workers.wait_for_complete()
                    await pilot.pause()

                    assert keys_of(app, "workers") == [worker.worker_id]
                    assert rows_of(app, "tasks")[0][0] == "the-named-one"
                    assert "total 1" in text_of(app.query_one("#summary", Static))

        drive(scenario)
        worker.close()

    def test_refreshing_picks_up_what_changed(
        self, executor, ds_service_address, tmp_path
    ):
        """`r` polls now rather than at the next interval."""

        async def scenario():
            async with open_collector(ds_service_address) as collector:
                app = SwtopApp(collector, 3600.0)
                async with app.run_test() as pilot:
                    await app.workers.wait_for_complete()
                    await pilot.pause()
                    assert table_of(app, "workers").row_count == 0

                    worker = make_worker(
                        ds_service_address, tmp_path, group="cpu", name="w-1"
                    )
                    try:
                        await pilot.press("r")
                        await app.workers.wait_for_complete()
                        await pilot.pause()

                        assert keys_of(app, "workers") == [worker.worker_id]
                    finally:
                        worker.close()

        drive(scenario)

    def test_an_unreachable_server_is_reported_not_fatal(self):
        """A monitor that quits when the server blinks is not much of one."""

        async def scenario():
            async with open_collector("127.0.0.1:1") as collector:
                app = SwtopApp(collector, 3600.0)
                async with app.run_test() as pilot:
                    await app.workers.wait_for_complete()
                    await pilot.pause()

                    assert app.is_running
                    assert app.query_one("#error", Static).display

        drive(scenario)

    def test_quitting_ends_it(self, ds_service_address):
        async def scenario():
            async with open_collector(ds_service_address) as collector:
                app = SwtopApp(collector, 60.0)
                async with app.run_test() as pilot:
                    await app.workers.wait_for_complete()

                    await pilot.press("q")
                    await pilot.pause()

                    assert not app.is_running

        drive(scenario)
