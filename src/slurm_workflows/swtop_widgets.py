"""The `swtop` widgets, for `swtop` and for any Textual app that embeds it."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import datetime
from typing import Any, Protocol

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.message import Message
from textual.widget import Widget
from textual.widgets import (
    Checkbox,
    DataTable,
    ProgressBar,
    Static,
    TabbedContent,
    TabPane,
)

from .swtop import (
    BLOCKS,
    DEFAULT_INTERVAL_S,
    DEFAULT_TASK_STATES,
    EMPTY_TASKS_IN_STATES,
    STATE_ORDER,
    BlockSpec,
    Collector,
    Snapshot,
    block_label,
    counts_line,
    open_collector,
)


class SnapshotView(Protocol):
    """A widget that shows what it can of one snapshot."""

    def show(self, snapshot: Snapshot) -> None:
        """Draw what the view can of `snapshot`,
        which may be a failed poll with `error` set and every reading empty.
        """
        ...


def sync_table(table: DataTable, rows: list[tuple[str, list[str]]]) -> None:
    """Bring one table to `rows`, and change only what differs.

    Each row keeps the key it carries in `rows`.
    So a row that is still there keeps its place,
    its scroll position and the cursor on it.
    A new row goes at the bottom,
    so the order of the table can drift from the order of `rows`.
    """
    wanted = {key: cells for key, cells in rows}
    columns = list(table.columns)

    # Never `DataTable.clear()`, which loses the scroll position and the cursor.
    for key in [str(row.value) for row in table.rows]:
        if key not in wanted:
            table.remove_row(key)

    for key, cells in rows:
        if key in {str(row.value) for row in table.rows}:
            for column, cell in zip(columns, cells):
                if table.get_cell(key, column) != cell:
                    table.update_cell(key, column, cell, update_width=True)
        else:
            table.add_row(*cells, key=key)


# Every view below ignores a failed poll except `ErrorLine`.
# See "`swtop` draws a failed poll" in the developer notes.


class SummaryLine(Static):
    """The task counts, and when the server was last read."""

    DEFAULT_CSS = """
    SummaryLine {
        padding: 0 1;
        color: $text-muted;
    }
    """

    def show(self, snapshot: Snapshot) -> None:
        """Draw the counts of a good poll."""
        if snapshot.error is not None:
            return
        when = snapshot.when.strftime("%H:%M:%S")
        self.update(f"{counts_line(snapshot)}   {when}")


class ErrorLine(Static):
    """Why the last poll failed, hidden while polls succeed."""

    DEFAULT_CSS = """
    ErrorLine {
        display: none;
        padding: 0 1;
        color: $error;
    }
    """

    def show(self, snapshot: Snapshot) -> None:
        """Draw the error of a failed poll, or hide after a good one."""
        self.display = snapshot.error is not None
        if snapshot.error is not None:
            self.update(f"cannot read the server: {snapshot.error}")


class ProgressDisplay(Vertical):
    """The bar that shows a `wait` or `as_completed` call."""

    DEFAULT_CSS = """
    ProgressDisplay {
        display: none;
        height: auto;
        padding: 0 1;
    }
    ProgressDisplay .swtop-progress-label { color: $text-muted; }
    """

    def compose(self) -> ComposeResult:
        yield Static("", classes="swtop-progress-label")
        # A placeholder until `show` sets the real total.
        yield ProgressBar(total=100, show_eta=False)

    def show(self, snapshot: Snapshot) -> None:
        """Draw the progress of a good poll, or hide when there is none."""
        if snapshot.error is not None:
            return
        progress = snapshot.progress
        self.display = progress is not None
        if progress is None:
            return

        state = "done" if progress.done else "working"
        self.query_one(".swtop-progress-label", Static).update(
            f"{progress.desc}  "
            f"{progress.completed}/{progress.total} {progress.unit}  {state}"
        )
        bar = self.query_one(ProgressBar)
        # `max` keeps a zero total, which an empty wait publishes, away from the bar.
        # The bar then reads 0%, where `fraction` and the text display read done.
        bar.update(total=max(progress.total, 1), progress=progress.completed)


class BlockTable(Vertical):
    """The rows of one block, or what the block says when it is empty.

    The block's title and count are not drawn here.
    They belong in the label of the tab that holds the table,
    so the table posts `CountChanged` for whatever holds it.
    """

    DEFAULT_CSS = """
    BlockTable { height: 1fr; }
    BlockTable DataTable { height: 1fr; }
    BlockTable .swtop-block-empty {
        padding: 0 1;
        color: $text-muted;
    }
    """

    class CountChanged(Message):
        """The number of rows in a `BlockTable` changed."""

        def __init__(self, table: BlockTable, count: int) -> None:
            super().__init__()
            self.table = table
            self.count = count

        @property
        def spec(self) -> BlockSpec:
            """The block whose count changed."""
            return self.table.spec

    def __init__(self, spec: BlockSpec, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.spec = spec
        self.count = 0
        # The last good snapshot, which `redraw` draws.
        self.snapshot: Snapshot | None = None

    def compose(self) -> ComposeResult:
        yield Static(self.spec.empty, classes="swtop-block-empty")
        yield DataTable(zebra_stripes=True, cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns(*self.spec.columns)
        table.display = False

    def show(self, snapshot: Snapshot) -> None:
        """Put the block's rows in the table, or say why there are none."""
        if snapshot.error is not None:
            return
        self.snapshot = snapshot
        self.redraw()

    def rows(self, snapshot: Snapshot) -> list[tuple[str, list[str]]]:
        """The rows of `snapshot` that this table shows."""
        return self.spec.rows(snapshot)

    def empty_text(self, snapshot: Snapshot) -> str:
        """What the table says when `snapshot` gives it no rows."""
        return self.spec.empty

    def redraw(self) -> None:
        """Draw the last good snapshot again, or nothing before the first one.

        `show` calls it after each good poll.
        A subclass calls it when what it picks out of a snapshot changes.
        """
        if self.snapshot is None:
            return
        rows = self.rows(self.snapshot)
        table = self.query_one(DataTable)
        sync_table(table, rows)

        # One or the other: a header row over nothing reads as a failure.
        table.display = bool(rows)
        empty = self.query_one(".swtop-block-empty", Static)
        empty.display = not rows
        if not rows:
            empty.update(self.empty_text(self.snapshot))

        if len(rows) != self.count:
            self.count = len(rows)
            self.post_message(self.CountChanged(self, self.count))


class TaskStateFilter(Horizontal):
    """A checkbox for each task state, which picks the states a `TaskTable` shows.

    `states` are the states checked at start.
    The checkboxes come in the order of `STATE_ORDER`.
    Each change posts `Changed`.
    """

    DEFAULT_CSS = """
    TaskStateFilter {
        height: auto;
        padding: 0 1;
    }
    TaskStateFilter Checkbox {
        border: none;
        padding: 0 1 0 0;
        background: transparent;
    }
    TaskStateFilter Checkbox:focus { border: none; }
    """

    class Changed(Message):
        """The viewer checked or unchecked a state."""

        def __init__(self, state_filter: TaskStateFilter) -> None:
            super().__init__()
            self.state_filter = state_filter

        @property
        def states(self) -> frozenset[str]:
            """The states now checked."""
            return self.state_filter.states

    def __init__(
        self, states: Iterable[str] = DEFAULT_TASK_STATES, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._initial = frozenset(states)

    def compose(self) -> ComposeResult:
        # The name, not an id, carries the state,
        # so two filters on one screen do not clash.
        for state in STATE_ORDER:
            yield Checkbox(state, state in self._initial, name=state)

    @property
    def states(self) -> frozenset[str]:
        """The states now checked."""
        return frozenset(
            box.name
            for box in self.query(Checkbox)
            if box.value and box.name is not None
        )

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        # One message of this widget's own in place of the checkbox's.
        event.stop()
        self.post_message(self.Changed(self))


class TaskTable(BlockTable):
    """A `BlockTable` for the tasks block, with a `TaskStateFilter` above the rows.

    It shows only the tasks in `states`, which follow its checkboxes.
    Its count, and so the tab label, is the number of tasks it shows.
    """

    def __init__(
        self,
        spec: BlockSpec,
        states: Iterable[str] = DEFAULT_TASK_STATES,
        **kwargs: Any,
    ) -> None:
        super().__init__(spec, **kwargs)
        self.states = frozenset(states)

    def compose(self) -> ComposeResult:
        yield TaskStateFilter(self.states)
        yield from super().compose()

    def rows(self, snapshot: Snapshot) -> list[tuple[str, list[str]]]:
        """The rows of the tasks in `states`."""
        shown = [t for t in snapshot.tasks if t.state in self.states]
        return self.spec.rows(replace(snapshot, tasks=shown))

    def empty_text(self, snapshot: Snapshot) -> str:
        """Why no task shows: there are none, or none in `states`."""
        return self.spec.empty if not snapshot.tasks else EMPTY_TASKS_IN_STATES

    def on_task_state_filter_changed(self, event: TaskStateFilter.Changed) -> None:
        # The message goes on bubbling, for an app that wants the states too.
        self.states = event.states
        self.redraw()


# The table `block_table` makes for a block, by block key,
# where it is not a plain `BlockTable`.
BLOCK_TABLES: dict[str, type[BlockTable]] = {"tasks": TaskTable}


def block_table(spec: BlockSpec, **kwargs: Any) -> BlockTable:
    """A `TaskTable` for the tasks block, and a `BlockTable` for any other."""
    return BLOCK_TABLES.get(spec.key, BlockTable)(spec, **kwargs)


class BlockPane(TabPane):
    """A tab that holds the table of one block, and keeps its count in its label."""

    DEFAULT_CSS = """
    BlockPane { padding: 0; }
    """

    def __init__(self, spec: BlockSpec, **kwargs: Any) -> None:
        super().__init__(block_label(spec, 0), block_table(spec), **kwargs)
        self.spec = spec

    def on_block_table_count_changed(self, event: BlockTable.CountChanged) -> None:
        # A `TabPane` has no way to change its own label in Textual 8.2,
        # so ask the `TabbedContent` it sits in for its tab.
        # The message goes on bubbling, for an app that wants the count too.
        try:
            tabs = self.query_ancestor(TabbedContent)
        except NoMatches:
            # Not in a `TabbedContent` yet, so there is no label to change.
            return
        tabs.get_tab(self).label = block_label(self.spec, event.count)


def block_pane(spec: BlockSpec, *, id: str | None = None) -> BlockPane:
    """A tab that holds the `block_table` for `spec`, labeled with its count.

    The id defaults to `swtop-` and the block key, such as `swtop-workers`.
    """
    return BlockPane(spec, id=id or f"swtop-{spec.key}")


class SwtopTabs(TabbedContent):
    """One tab for each block, in the order of `BLOCKS`."""

    DEFAULT_CSS = """
    SwtopTabs { height: 1fr; }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # `TabbedContent.compose` builds the tab row
        # from what `compose_add_child` gathered,
        # which is how a `with TabbedContent():` block hands it its panes.
        # Panes yielded from a `compose` override get no tab row.
        for spec in BLOCKS:
            self.compose_add_child(block_pane(spec))


class SnapshotPoller(Widget):
    """Polls one server on an interval, and shows each snapshot on its views.

    It takes exactly one of `address` and `collector`.
    With `address`, the poller opens a collector of its own on mount,
    on the app's event loop, and closes it on unmount.
    With `collector`, the caller owns the collector,
    which must belong to the app's event loop.
    Construction raises `ValueError` if both or neither are given,
    or if `interval` is not greater than 0.

    After each poll the poller calls `show` on every attached view,
    and then posts `Polled`.
    It draws nothing and binds no key.
    """

    DEFAULT_CSS = """
    SnapshotPoller { display: none; }
    """

    class Polled(Message):
        """A poll finished, well or with its `error` set."""

        def __init__(self, poller: SnapshotPoller, snapshot: Snapshot) -> None:
            super().__init__()
            self.poller = poller
            self.snapshot = snapshot

    def __init__(
        self,
        *,
        address: str | None = None,
        collector: Collector | None = None,
        interval: float = DEFAULT_INTERVAL_S,
        views: Iterable[SnapshotView] = (),
        **kwargs: Any,
    ) -> None:
        if (address is None) == (collector is None):
            raise ValueError("give exactly one of address and collector")
        if interval <= 0:
            raise ValueError("interval must be greater than 0")
        super().__init__(**kwargs)
        self.address = collector.address if collector is not None else address
        self.collector = collector
        self.interval = interval
        # Views come only through `attach`, never a DOM query,
        # which would also find a second poller's views.
        self.views: list[SnapshotView] = list(views)
        # The last snapshot, which a view attached later starts from.
        self.snapshot: Snapshot | None = None
        self._stack = AsyncExitStack() if collector is None else None
        # Whether a poll is in flight,
        # and whether `poll_now` asked for another while it was.
        self._polling = False
        self._again = False

    def attach(self, *views: SnapshotView) -> None:
        """Show every later snapshot on `views` too.

        A view that is attached after a poll shows that poll's snapshot at once,
        so it does not wait for the next interval.
        A widget must be mounted before it is attached.
        """
        new = [v for v in views if v not in self.views]
        self.views.extend(new)
        if self.snapshot is not None:
            for view in new:
                view.show(self.snapshot)

    def detach(self, *views: SnapshotView) -> None:
        """Stop showing snapshots on `views`."""
        self.views = [v for v in self.views if v not in views]

    async def on_mount(self) -> None:
        if self._stack is not None:
            assert self.address is not None
            self.collector = await self._stack.enter_async_context(
                open_collector(self.address)
            )
        self.poll_now()
        self.set_interval(self.interval, self._tick)

    async def on_unmount(self) -> None:
        # Cancel a poll in flight before closing the client it reads through.
        self.workers.cancel_node(self)
        if self._stack is not None:
            await self._stack.aclose()

    def poll_now(self) -> None:
        """Poll now rather than at the next interval, without blocking.

        A poll in flight is never cut short.
        A call while one is in flight polls once more when it ends,
        so what was asked for still reads the server afresh.
        """
        if self._polling:
            self._again = True
            return

        # One poll at a time, and each one runs to its end.
        # See "`SnapshotPoller` lets a slow poll finish" in the developer notes.
        self._polling = True
        # The group is per poller, so two pollers leave each other alone.
        self.run_worker(self._poll, group=f"swtop-poll-{id(self)}")

    def _tick(self) -> None:
        """Poll on the interval, unless a poll is still in flight."""
        # A tick during a slow poll is dropped, not queued.
        # See "`SnapshotPoller` lets a slow poll finish" in the developer notes.
        if not self._polling:
            self.poll_now()

    def deliver(self, snapshot: Snapshot) -> None:
        """Show `snapshot` on every view, and post `Polled`.

        A test can call it to draw a snapshot of its own.
        """
        self.snapshot = snapshot
        for view in self.views:
            view.show(snapshot)
        self.post_message(self.Polled(self, snapshot))

    async def _poll(self) -> None:
        """One poll, awaited in a worker so the interface does not block."""
        assert self.collector is not None
        try:
            try:
                snapshot = await self.collector.snapshot()
            except Exception as e:
                # See "`swtop` draws a failed poll" in the developer notes.
                snapshot = Snapshot(
                    address=self.collector.address,
                    when=datetime.now(),
                    error=f"{type(e).__name__}: {e}",
                )

            self.deliver(snapshot)
        finally:
            self._polling = False

        # Not reached when unmounting cancels the poll,
        # so a poller on its way out starts nothing new.
        if self._again:
            self._again = False
            self.poll_now()
