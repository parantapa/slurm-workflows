"""Tests for the `swtop` terminal UI.

The tests assert on the state of the widgets rather than on pixels.
How they drive the app headlessly, and why they wait on `app.workers`,
is in how-to-run-tests.md, under "Notes for future changes".
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    ProgressBar,
    Static,
    TabbedContent,
    TabPane,
)

from slurm_workflows.swtop import (
    BLOCKS,
    Collector,
    Snapshot,
    SubjectInfo,
    ProgressInfo,
    TaskInfo,
    WorkerInfo,
    PilotJobInfo,
    open_collector,
)
from slurm_workflows.swtop_tui import TAB_KEYS, SwtopApp
from slurm_workflows.swtop_widgets import (
    BlockTable,
    ErrorLine,
    ProgressDisplay,
    SnapshotPoller,
    SummaryLine,
    BlockPane,
    SwtopTabs,
    block_pane,
    sync_table,
)
from worker_harness import make_worker


def square(x: int) -> int:
    return x * x


def text_of(widget: Static) -> str:
    """What a `Static` shows now."""
    return str(widget.content)


def table_of(app: App, block: str) -> DataTable:
    """The table of the block with key `block`, found by its tab."""
    return app.query_one(f"#swtop-{block}").query_one(DataTable)


def label_of(app: App, block: str) -> str:
    """What the tab of the block with key `block` says."""
    return app.query_one(TabbedContent).get_tab(f"swtop-{block}").label.plain


def rows_of(app: App, block: str) -> list[list[str]]:
    table = table_of(app, block)
    return [table.get_row(key) for key in table.rows]


def keys_of(app: App, block: str) -> list[str]:
    return [str(key.value) for key in table_of(app, block).rows]


def snapshot(**kwargs) -> Snapshot:
    """A snapshot with everything filled in unless a test says otherwise."""
    filled = Snapshot(
        address="host:1",
        when=datetime.now(),
        counts={"ready": 1, "running": 2, "finished": 3, "canceled": 0},
        progress=ProgressInfo("p-1", "explore", "point", 8, 2),
        worker_jobs=[
            PilotJobInfo("run.job.cpu.0", "cpu", "42", "2026-09-07T11:04:57-04:00")
        ],
        workers=[WorkerInfo("w-id", "cpu", "run.job.cpu.0", "42", "node-1", "17")],
        hosts=[
            SubjectInfo("node-1", {"free_memory": 2 * 1024**3, "load_average": 3.5})
        ],
        jobs=[SubjectInfo("42", {"memory": 1024**3, "cpu": 12.4})],
        tasks=[TaskInfo("run.task.0", "train-7", "Running", "run.job.cpu.0")],
    )
    return replace(filled, **kwargs)


def drive(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    """Run one async scenario against a fresh app."""
    asyncio.run(scenario())


class StubCollector:
    """Hands back one snapshot, or fails, without a server behind it."""

    def __init__(
        self, snapshot: Snapshot | None = None, address: str = "host:1"
    ) -> None:
        self.address = address
        self._snapshot = snapshot

    async def snapshot(self) -> Snapshot:
        if self._snapshot is None:
            raise TimeoutError("nothing to give")
        return self._snapshot


def as_collector(stub: StubCollector) -> Collector:
    """Type the stand-in as the collector it stands in for."""
    # The app and `SnapshotPoller` use only `snapshot()` and `address`,
    # and `StubCollector` provides both.
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

# The tests here start the app on a `StubCollector` with no snapshot,
# so the startup poll fails.
# Each test waits for that poll to end before it delivers its own snapshot,
# so the failure cannot land on top of it.
# The 3600 s interval keeps a second poll from firing during a test.


class TestDisplay:
    def test_every_block_is_filled_in(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())

                assert rows_of(app, "pilot-jobs") == [
                    ["run.job.cpu.0", "cpu", "42", "2026-09-07T11:04:57-04:00"]
                ]
                assert rows_of(app, "workers") == [
                    ["run.job.cpu.0", "cpu", "node-1", "42", "17"]
                ]
                assert rows_of(app, "hosts")[0][:3] == ["node-1", "2.0G", "3.50"]
                assert rows_of(app, "jobs") == [["42", "1.0G", "12.4 cores"]]
                assert rows_of(app, "tasks") == [
                    ["train-7", "run.task.0", "Running", "run.job.cpu.0"]
                ]

        drive(scenario)

    def test_a_job_with_no_process_shows_in_one_block_only(self):
        """A queued pilot job: submitted, and nothing running in it yet."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot(workers=[]))

                assert len(rows_of(app, "pilot-jobs")) == 1
                assert rows_of(app, "workers") == []
                empty = app.query_one("#swtop-workers").query_one(
                    ".swtop-block-empty", Static
                )
                assert "no workers have registered" in text_of(empty)

        drive(scenario)

    def test_the_progress_display_is_a_bar(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())

                block = app.query_one("#progress")
                assert block.display
                label = text_of(block.query_one(".swtop-progress-label", Static))
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
                app.poller.deliver(snapshot(progress=None))

                assert not app.query_one("#progress").display

        drive(scenario)

    def test_the_summary_carries_the_counts(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())

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
                app.poller.deliver(snapshot(hosts=[], workers=[], jobs=[], tasks=[]))

                block = app.query_one("#swtop-hosts")
                assert not block.query_one(DataTable).display
                empty = block.query_one(".swtop-block-empty", Static)
                assert empty.display
                assert "no host is being monitored" in text_of(empty)

        drive(scenario)

    def test_a_tab_counts_its_rows_in_its_label(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(
                    snapshot(
                        workers=[
                            WorkerInfo(f"id-{i}", "cpu", f"w-{i}", "42", "node", "1")
                            for i in range(3)
                        ]
                    )
                )

                await pilot.pause()

                assert label_of(app, "workers") == "workers (3)"

                app.poller.deliver(snapshot(workers=[]))
                await pilot.pause()

                assert label_of(app, "workers") == "workers (0)"

        drive(scenario)

    def test_rows_keep_their_identity_across_updates(self):
        """The reason for keying: a scroll position survives a poll."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())
                first = keys_of(app, "workers")

                app.poller.deliver(snapshot())

                assert keys_of(app, "workers") == first

        drive(scenario)

    def test_a_worker_that_appears_is_added_to_the_table(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())

                more = [
                    WorkerInfo("w-id", "cpu", "run.job.cpu.0", "42", "node-1", "17"),
                    WorkerInfo("w-id2", "cpu", "run.job.cpu.1", "42", "node-2", "18"),
                ]
                app.poller.deliver(snapshot(workers=more))

                assert keys_of(app, "workers") == ["w-id", "w-id2"]

        drive(scenario)


class TestFailedPoll:
    def test_it_is_reported_on_the_screen(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot(error="TimeoutError: server unreachable"))

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
                app.poller.deliver(snapshot())

                app.poller.deliver(snapshot(error="TimeoutError: server unreachable"))

                assert rows_of(app, "workers") == [
                    ["run.job.cpu.0", "cpu", "node-1", "42", "17"]
                ]

        drive(scenario)

    def test_a_good_poll_clears_the_error(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot(error="TimeoutError: server unreachable"))

                app.poller.deliver(snapshot())

                assert not app.query_one("#error", Static).display

        drive(scenario)

    def test_a_failed_poll_leaves_the_summary_and_the_progress(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot())

                app.poller.deliver(snapshot(error="TimeoutError: server unreachable"))

                assert "total 6" in text_of(app.query_one("#summary", Static))
                assert app.query_one("#progress").display

        drive(scenario)


# --------------------------------------------------------------------------
# The layout and the keys of the app
# --------------------------------------------------------------------------


class TestLayout:
    def test_the_areas_come_in_order(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test():
                areas = [
                    type(w)
                    for w in app.screen.children
                    if not isinstance(w, SnapshotPoller)
                ]
                assert areas == [
                    Header,
                    SummaryLine,
                    SwtopTabs,
                    ProgressDisplay,
                    ErrorLine,
                    Footer,
                ]

        drive(scenario)

    def test_there_is_one_tab_for_each_block_in_order(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test():
                panes = [pane.id for pane in app.query_one(SwtopTabs).query(TabPane)]
                assert panes == [f"swtop-{spec.key}" for spec in BLOCKS]
                assert label_of(app, "jobs") == "slurm jobs (0)"

        drive(scenario)

    def test_every_block_has_its_own_tab_key(self):
        assert sorted(TAB_KEYS) == sorted(spec.key for spec in BLOCKS)
        assert len(set(TAB_KEYS.values())) == len(TAB_KEYS)
        # `q` and `r` are the app's own quit and refresh keys.
        assert not {"q", "r"} & set(TAB_KEYS.values())

    def test_each_tab_key_shows_its_tab(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                for block, key in TAB_KEYS.items():
                    await pilot.press(key)
                    await pilot.pause()

                    assert app.query_one(SwtopTabs).active == f"swtop-{block}"

        drive(scenario)

    def test_no_embeddable_widget_binds_a_key(self):
        """Keys belong to the app that lays the widgets out."""
        for widget in [
            SummaryLine,
            ErrorLine,
            ProgressDisplay,
            BlockTable,
            BlockPane,
            SwtopTabs,
            SnapshotPoller,
        ]:
            assert "BINDINGS" not in vars(widget), widget.__name__


# --------------------------------------------------------------------------
# Embedded in another app
# --------------------------------------------------------------------------


class HostApp(App):
    """An app of someone else's, with two blocks among its own tabs."""

    def __init__(self, collector: Collector) -> None:
        super().__init__()
        self.collector = collector
        self.polled: list[Snapshot] = []

    def compose(self) -> ComposeResult:
        yield SummaryLine()
        with TabbedContent():
            with TabPane("mine", id="mine"):
                yield Static("the host's own view")
            for spec in BLOCKS:
                if spec.key in {"workers", "tasks"}:
                    yield block_pane(spec)
        yield ProgressDisplay()
        yield ErrorLine()
        yield SnapshotPoller(collector=self.collector, interval=3600.0)

    def on_mount(self) -> None:
        self.query_one(SnapshotPoller).attach(
            self.query_one(SummaryLine),
            self.query_one(ProgressDisplay),
            self.query_one(ErrorLine),
            *self.query(BlockTable),
        )

    def on_snapshot_poller_polled(self, event: SnapshotPoller.Polled) -> None:
        self.polled.append(event.snapshot)


class StatusApp(App):
    """An app that embeds the lines around the tabs, and no tabs."""

    def __init__(self, collector: Collector) -> None:
        super().__init__()
        self.collector = collector

    def compose(self) -> ComposeResult:
        yield SummaryLine()
        yield ProgressDisplay()
        yield ErrorLine()
        yield SnapshotPoller(collector=self.collector, interval=3600.0)

    def on_mount(self) -> None:
        self.query_one(SnapshotPoller).attach(
            self.query_one(SummaryLine),
            self.query_one(ProgressDisplay),
            self.query_one(ErrorLine),
        )


class TwoServerApp(App):
    """One app that watches two servers, each on a poller of its own."""

    def __init__(self, first: Collector, second: Collector) -> None:
        super().__init__()
        self.first = first
        self.second = second

    def compose(self) -> ComposeResult:
        workers = next(spec for spec in BLOCKS if spec.key == "workers")
        yield BlockTable(workers, id="first-workers")
        yield BlockTable(workers, id="second-workers")
        yield SnapshotPoller(collector=self.first, interval=3600.0, id="first")
        yield SnapshotPoller(collector=self.second, interval=3600.0, id="second")

    def on_mount(self) -> None:
        self.query_one("#first", SnapshotPoller).attach(
            self.query_one("#first-workers", BlockTable)
        )
        self.query_one("#second", SnapshotPoller).attach(
            self.query_one("#second-workers", BlockTable)
        )


class TestEmbedding:
    def test_block_tabs_fill_in_among_the_hosts_own(self):
        async def scenario():
            app = HostApp(as_collector(StubCollector(snapshot())))
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                panes = [pane.id for pane in app.query(TabPane)]
                assert panes == ["mine", "swtop-workers", "swtop-tasks"]
                assert rows_of(app, "workers") == [
                    ["run.job.cpu.0", "cpu", "node-1", "42", "17"]
                ]
                assert label_of(app, "workers") == "workers (1)"
                assert label_of(app, "tasks") == "tasks (1)"

        drive(scenario)

    def test_the_lines_fill_in_around_the_hosts_tabs(self):
        async def scenario():
            app = HostApp(as_collector(StubCollector(snapshot())))
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                assert "total 6" in text_of(app.query_one(SummaryLine))
                assert app.query_one(ProgressDisplay).display
                assert not app.query_one(ErrorLine).display

        drive(scenario)

    def test_the_host_hears_of_each_poll(self):
        async def scenario():
            app = HostApp(as_collector(StubCollector(snapshot())))
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                assert len(app.polled) == 1
                assert app.polled[0].error is None

        drive(scenario)

    def test_the_host_gets_no_tab_keys(self):
        async def scenario():
            app = HostApp(as_collector(StubCollector(snapshot())))
            async with app.run_test() as pilot:
                for key in TAB_KEYS.values():
                    await pilot.press(key)
                await pilot.pause()

                assert app.query_one(TabbedContent).active == "mine"

        drive(scenario)

    def test_the_lines_fill_in_without_any_tabs(self):
        async def scenario():
            app = StatusApp(as_collector(StubCollector()))
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                error = app.query_one(ErrorLine)
                assert error.display
                assert "cannot read the server" in text_of(error)

        drive(scenario)

    def test_the_widgets_bring_their_own_styles(self):
        """Hidden before the first poll, with no CSS from the app."""

        class BareApp(App):
            def compose(self) -> ComposeResult:
                yield ErrorLine()
                yield ProgressDisplay()
                yield SnapshotPoller(
                    collector=as_collector(StubCollector()), interval=3600.0
                )

        async def scenario():
            app = BareApp()
            async with app.run_test():
                assert not app.query_one(ErrorLine).display
                assert not app.query_one(ProgressDisplay).display
                assert not app.query_one(SnapshotPoller).display

        drive(scenario)

    def test_two_pollers_keep_to_their_own_views(self):
        async def scenario():
            other = snapshot(
                workers=[WorkerInfo("w-2", "gpu", "run.job.gpu.0", "43", "node-2", "9")]
            )
            app = TwoServerApp(
                as_collector(StubCollector(snapshot())),
                as_collector(StubCollector(other, address="host:2")),
            )
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                first = app.query_one("#first-workers").query_one(DataTable)
                second = app.query_one("#second-workers").query_one(DataTable)
                assert [str(k.value) for k in first.rows] == ["w-id"]
                assert [str(k.value) for k in second.rows] == ["w-2"]

        drive(scenario)

    def test_a_poller_takes_one_source(self):
        with pytest.raises(ValueError):
            SnapshotPoller()
        with pytest.raises(ValueError):
            SnapshotPoller(address="host:1", collector=as_collector(StubCollector()))


# --------------------------------------------------------------------------
# Against a real server
# --------------------------------------------------------------------------


class TestRealServer:
    """The polling half: a Textual worker, a real client, and the widgets it fills."""

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

    def test_a_poller_with_an_address_opens_its_own_collector(
        self, executor, ds_service_address
    ):
        executor.submit("cpu", square, 5)

        class AddressApp(App):
            def compose(self) -> ComposeResult:
                yield SummaryLine()
                yield SnapshotPoller(address=ds_service_address, interval=3600.0)

            def on_mount(self) -> None:
                self.query_one(SnapshotPoller).attach(self.query_one(SummaryLine))

        async def scenario():
            app = AddressApp()
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()

                assert "total 1" in text_of(app.query_one(SummaryLine))

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
