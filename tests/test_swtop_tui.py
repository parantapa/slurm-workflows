"""Tests for the `swtop` terminal UI."""

# The tests assert on the state of the widgets rather than on pixels.
# How they drive the app headlessly, and why they wait on `app.workers`:
# see docs/how-to-run-tests.md, under "Notes for future changes".

from __future__ import annotations

import asyncio
from dataclasses import replace
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Callable, Coroutine
from datetime import datetime
from typing import Any, cast

import pytest
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import (
    Checkbox,
    DataTable,
    ProgressBar,
    Static,
    TabbedContent,
    TabPane,
)

from slurm_workflows.swtop import (
    BLOCKS,
    DEFAULT_TASK_STATES,
    EMPTY_TASKS,
    EMPTY_TASKS_IN_STATES,
    STATE_ORDER,
    Collector,
    Snapshot,
    SubjectInfo,
    ProgressInfo,
    TaskInfo,
    WorkerInfo,
    PilotJobInfo,
    open_collector,
)
from slurm_workflows import swtop_widgets
from slurm_workflows.swtop_tui import TAB_KEYS, SwtopApp
from slurm_workflows.swtop_widgets import (
    BlockTable,
    ErrorLine,
    ProgressDisplay,
    SnapshotPoller,
    SummaryLine,
    BlockPane,
    SwtopTabs,
    TaskStateFilter,
    TaskTable,
    block_pane,
    sync_table,
)
from slurm_workflows.testing import make_worker


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
            PilotJobInfo(
                "run.job.cpu.0",
                "cpu",
                "42",
                "2026-09-07T11:04:57-04:00",
                "2026-09-07T11:05:30-04:00",
            )
        ],
        workers=[
            WorkerInfo(
                "w-id",
                "cpu",
                "run.job.cpu.0",
                "42",
                "node-1",
                "17",
                "2026-09-07T11:05:41-04:00",
            )
        ],
        hosts=[
            SubjectInfo("node-1", {"free_memory": 2 * 1024**3, "load_average": 3.5})
        ],
        jobs=[SubjectInfo("42:node-1", {"memory": 1024**3, "cpu": 12.4})],
        tasks=[TaskInfo("run.task.0", "train-7", "Running", "run.job.cpu.0")],
    )
    return replace(filled, **kwargs)


def drive(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    """Run one async scenario against a fresh app."""
    asyncio.run(scenario())


class StubCollector:
    """Returns one snapshot, or fails, without a server behind it."""

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
                    [
                        "run.job.cpu.0",
                        "cpu",
                        "42",
                        "2026-09-07T11:04:57-04:00",
                        "2026-09-07T11:05:30-04:00",
                    ]
                ]
                assert rows_of(app, "workers") == [
                    [
                        "run.job.cpu.0",
                        "cpu",
                        "node-1",
                        "42",
                        "17",
                        "2026-09-07T11:05:41-04:00",
                    ]
                ]
                assert rows_of(app, "hosts")[0][:3] == ["node-1", "2.0G", "3.50"]
                assert rows_of(app, "jobs") == [["42", "node-1", "1.0G", "12.4 cores"]]
                assert rows_of(app, "tasks") == [
                    ["train-7", "run.task.0", "Running", "run.job.cpu.0"]
                ]

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

    def test_an_empty_wait_is_drawn_as_done(self):
        """A wait on no tasks publishes a total of 0, which must not divide."""

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                empty = ProgressInfo("p-0", "nothing", "task", 0)
                app.poller.deliver(snapshot(progress=empty))

                block = app.query_one("#progress")
                assert block.display
                label = text_of(block.query_one(".swtop-progress-label", Static))
                assert "0/0 task" in label and "done" in label
                bar = block.query_one(ProgressBar)
                assert (bar.total, bar.progress) == (1, 0)

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

                # The tab label follows a message the table posts,
                # so let the app handle it.
                await pilot.pause()

                assert label_of(app, "workers") == "workers (3)"

                app.poller.deliver(snapshot(workers=[]))
                await pilot.pause()

                assert label_of(app, "workers") == "workers (0)"

        drive(scenario)


def tasks_in_every_state() -> list[TaskInfo]:
    """One task in each state `STATE_ORDER` names, in that order."""
    return [
        TaskInfo(f"run.task.{i}", f"t-{state.lower()}", state)
        for i, state in enumerate(STATE_ORDER)
    ]


def state_box(app: App, state: str) -> Checkbox:
    """The checkbox of `state` in the tasks tab."""
    boxes = app.query_one(TaskStateFilter).query(Checkbox)
    return next(box for box in boxes if box.name == state)


class TestTaskStateFilter:
    def test_there_is_a_checkbox_for_each_state_in_order(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test():
                boxes = app.query_one(TaskStateFilter).query(Checkbox)
                assert [box.name for box in boxes] == STATE_ORDER

        drive(scenario)

    def test_it_starts_on_the_tasks_still_to_finish(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot(tasks=tasks_in_every_state()))
                await pilot.pause()

                assert [row[2] for row in rows_of(app, "tasks")] == [
                    "Running",
                    "Ready",
                    "Waiting",
                ]
                assert label_of(app, "tasks") == "tasks (3)"
                checked = {box.name for box in app.query(Checkbox) if box.value}
                assert checked == set(DEFAULT_TASK_STATES)

        drive(scenario)

    def test_checking_or_unchecking_a_state_redraws_at_once(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                app.poller.deliver(snapshot(tasks=tasks_in_every_state()))

                # No poll in between: the table redraws the last one.
                state_box(app, "Failed").value = True
                await pilot.pause()

                assert "Failed" in [row[2] for row in rows_of(app, "tasks")]
                assert label_of(app, "tasks") == "tasks (4)"

                state_box(app, "Failed").value = False
                state_box(app, "Running").value = False
                await pilot.pause()

                assert [row[2] for row in rows_of(app, "tasks")] == [
                    "Ready",
                    "Waiting",
                ]

        drive(scenario)

    def test_the_choice_holds_across_polls(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                state_box(app, "Finished").value = True
                await pilot.pause()

                app.poller.deliver(snapshot(tasks=tasks_in_every_state()))

                assert "Finished" in [row[2] for row in rows_of(app, "tasks")]

        drive(scenario)

    def test_tasks_in_no_chosen_state_say_so(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                done = [TaskInfo("run.task.0", "t-0", "Finished")]
                app.poller.deliver(snapshot(tasks=done))

                block = app.query_one("#swtop-tasks")
                assert not block.query_one(DataTable).display
                empty = block.query_one(".swtop-block-empty", Static)
                assert text_of(empty) == EMPTY_TASKS_IN_STATES

                app.poller.deliver(snapshot(tasks=[]))

                assert text_of(empty) == EMPTY_TASKS

        drive(scenario)

    def test_a_task_table_takes_its_own_starting_states(self):
        tasks = next(spec for spec in BLOCKS if spec.key == "tasks")

        class FinishedApp(App):
            def compose(self) -> ComposeResult:
                yield TaskTable(tasks, states={"Finished"})

        async def scenario():
            app = FinishedApp()
            async with app.run_test() as pilot:
                table = app.query_one(TaskTable)
                table.show(snapshot(tasks=tasks_in_every_state()))
                await pilot.pause()

                assert [row[2] for row in rows_of_table(table)] == ["Finished"]

        drive(scenario)


def rows_of_table(block: BlockTable) -> list[list[str]]:
    table = block.query_one(DataTable)
    return [table.get_row(key) for key in table.rows]


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
                    [
                        "run.job.cpu.0",
                        "cpu",
                        "node-1",
                        "42",
                        "17",
                        "2026-09-07T11:05:41-04:00",
                    ]
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
        # Every key binds to the action that shows its own tab.
        # Pressing all of them takes most of 2 s,
        # so two presses check that a bound action does what it names.
        bindings = Binding.make_bindings(SwtopApp.BINDINGS)
        actions = {binding.key: binding.action for binding in bindings}
        for block, key in TAB_KEYS.items():
            assert actions[key] == f"show_tab('swtop-{block}')"

        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            # A press waits out the tab underline's slide from the last press,
            # which takes about 0.3 s.
            app.animation_level = "none"
            async with app.run_test() as pilot:
                for block in ["tasks", "workers"]:
                    await pilot.press(TAB_KEYS[block])
                    await pilot.pause()

                    assert app.query_one(SwtopTabs).active == f"swtop-{block}"

        drive(scenario)

    def test_no_embeddable_widget_binds_a_key(self):
        """Keys belong to the app that arranges the widgets."""
        for widget in [
            SummaryLine,
            ErrorLine,
            ProgressDisplay,
            BlockTable,
            TaskTable,
            TaskStateFilter,
            BlockPane,
            SwtopTabs,
            SnapshotPoller,
        ]:
            assert "BINDINGS" not in vars(widget), widget.__name__


# --------------------------------------------------------------------------
# A poll slower than the interval
# --------------------------------------------------------------------------


class SlowCollector:
    """Takes `seconds` over every poll, and counts the polls it runs."""

    def __init__(self, seconds: float) -> None:
        self.address = "host:1"
        self.seconds = seconds
        self.started = 0
        self.finished = 0
        self.in_flight = 0
        self.most_in_flight = 0

    async def snapshot(self) -> Snapshot:
        self.started += 1
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.seconds)
        finally:
            self.in_flight -= 1
        self.finished += 1
        return snapshot()


class PollingApp(App):
    """A summary line on a poller of a given interval, and the polls it heard."""

    def __init__(self, collector: SlowCollector, interval: float) -> None:
        super().__init__()
        # `SlowCollector` provides the `snapshot()` and `address` a poller uses.
        self.collector = cast(Collector, collector)
        self.interval = interval
        self.polled: list[Snapshot] = []

    def compose(self) -> ComposeResult:
        yield SummaryLine()
        yield SnapshotPoller(collector=self.collector, interval=self.interval)

    def on_mount(self) -> None:
        self.query_one(SnapshotPoller).attach(self.query_one(SummaryLine))

    def on_snapshot_poller_polled(self, event: SnapshotPoller.Polled) -> None:
        self.polled.append(event.snapshot)


class TestSlowPoll:
    # A poll of 0.1 s against an interval of 0.02 s.
    # This is a read of a large pool through a tunnel, scaled down.

    def test_a_slow_poll_lands_and_no_other_overlaps_it(self):
        """The next tick neither cuts the poll short nor queues behind it."""
        slow = SlowCollector(0.1)

        async def scenario():
            app = PollingApp(slow, interval=0.02)
            async with app.run_test() as pilot:
                await pilot.pause(0.35)

                # The poll lands, so the screen moves on.
                assert slow.finished >= 2
                assert len(app.polled) == slow.finished
                assert "total 6" in text_of(app.query_one(SummaryLine))
                # The poller drops a tick during a poll.
                assert slow.most_in_flight == 1
                # About 0.35 / 0.1 polls. With queued ticks, the count is 17.
                assert slow.started <= 5

        drive(scenario)

    def test_a_refresh_asked_for_during_a_poll_runs_after_it(self):
        slow = SlowCollector(0.1)

        async def scenario():
            app = PollingApp(slow, interval=3600.0)
            async with app.run_test() as pilot:
                await pilot.pause(0.03)
                assert slow.started == 1

                app.query_one(SnapshotPoller).poll_now()
                await pilot.pause(0.03)
                assert slow.started == 1, "the first poll is not cut short"

                await pilot.pause(0.2)
                assert slow.started == 2
                assert slow.finished == 2

        drive(scenario)


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
                    [
                        "run.job.cpu.0",
                        "cpu",
                        "node-1",
                        "42",
                        "17",
                        "2026-09-07T11:05:41-04:00",
                    ]
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

    def test_a_detached_view_hears_of_no_more_polls(self):
        async def scenario():
            app = SwtopApp(as_collector(StubCollector()), 3600.0)
            async with app.run_test() as pilot:
                await app.workers.wait_for_complete()
                await pilot.pause()
                summary = app.query_one(SummaryLine)
                before = text_of(summary)

                app.poller.detach(summary)
                app.poller.deliver(snapshot())

                assert summary not in app.poller.views
                assert text_of(summary) == before
                assert "total 6" not in text_of(summary)
                assert len(rows_of(app, "workers")) == 1, "the others still hear"

        drive(scenario)

    def test_unmounting_cancels_the_poll_and_closes_its_collector(self, monkeypatch):
        """A poller that opened its own collector closes it on the way out."""
        slow = SlowCollector(3600.0)
        closed: list[int] = []

        @asynccontextmanager
        async def open_collector(address: str) -> AsyncIterator[SlowCollector]:
            try:
                yield slow
            finally:
                closed.append(slow.started)

        monkeypatch.setattr(swtop_widgets, "open_collector", open_collector)

        class AddressApp(PollingApp):
            def compose(self) -> ComposeResult:
                yield SummaryLine()
                yield SnapshotPoller(address="host:1", interval=3600.0)

        async def scenario():
            app = AddressApp(slow, interval=3600.0)
            async with app.run_test() as pilot:
                await pilot.pause()
                assert slow.in_flight == 1

                await app.query_one(SnapshotPoller).remove()
                await pilot.pause()

                assert closed == [1]
                assert slow.in_flight == 0, "the poll in flight is canceled"
                assert slow.finished == 0
                assert app.polled == []

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
    def _pilot_jobs(self, pilot_jobs: Callable[..., None]) -> None:
        pilot_jobs("cpu")

    def test_it_polls_on_startup(self, executor, ds_service_address, tmp_path):
        task = executor.submit("cpu", square, 5)
        executor.set_task_name(task, "the-named-one")
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        async def scenario():
            async with open_collector(ds_service_address) as collector:
                # The 60 s interval matches the 60 s alarm on every test,
                # so no second poll fires before the test ends.
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
                # The 60 s interval matches the 60 s alarm on every test,
                # so no second poll fires before the test ends.
                app = SwtopApp(collector, 60.0)
                async with app.run_test() as pilot:
                    await app.workers.wait_for_complete()

                    await pilot.press("q")
                    await pilot.pause()

                    assert not app.is_running

        drive(scenario)
