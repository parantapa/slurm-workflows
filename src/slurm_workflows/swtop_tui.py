"""The terminal UI `swtop` runs in, built with Textual.

`swtop.py` decides what to show.
This module puts it on a screen and keeps it there.
It polls in a Textual worker and updates the tables in place.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Footer, Header, ProgressBar, Static

from .swtop import (
    HOST_COLUMNS,
    ProgressInfo,
    JOB_COLUMNS,
    TASK_COLUMNS,
    WORKER_COLUMNS,
    WORKER_JOB_COLUMNS,
    EMPTY_HOSTS,
    EMPTY_JOBS,
    EMPTY_TASKS,
    EMPTY_WORKERS,
    EMPTY_WORKER_JOBS,
    Collector,
    Snapshot,
    counts_line,
    host_rows,
    job_rows,
    task_rows,
    worker_job_rows,
    worker_rows,
)


def sync_table(table: DataTable, rows: list[tuple[str, list[str]]]) -> None:
    """Bring one table to `rows`, and change only what differs.

    Each row keeps the key the builders hand out.
    So a row that is still there keeps its place,
    its scroll position and the cursor on it.
    """
    wanted = {key: cells for key, cells in rows}
    columns = list(table.columns)

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


class Block(VerticalScroll):
    """One titled table, and what it says when it is empty."""

    def __init__(
        self, title: str, columns: list[str], empty: str, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.title_text = title
        self.columns = columns
        self.empty_text = empty

    def compose(self) -> ComposeResult:
        yield Static(f"{self.title_text} (0)", classes="block-title")
        yield Static(self.empty_text, classes="block-empty")
        yield DataTable(zebra_stripes=True, cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns(*self.columns)
        table.display = False

    def show(self, rows: list[tuple[str, list[str]]]) -> None:
        """Put `rows` in the table, or say why there are none."""
        table = self.query_one(DataTable)
        sync_table(table, rows)

        self.query_one(".block-title", Static).update(
            f"{self.title_text} ({len(rows)})"
        )
        # One or the other: a header row over nothing reads as a failure.
        table.display = bool(rows)
        self.query_one(".block-empty", Static).display = not rows


class ProgressBlock(VerticalScroll):
    """The bar that shows a `wait` or `as_completed` call."""

    def compose(self) -> ComposeResult:
        yield Static("", classes="progress-label")
        yield ProgressBar(total=100, show_eta=False)

    def show(self, progress: ProgressInfo | None) -> None:
        """Draw one progress display, or hide the block when there is none."""
        self.display = progress is not None
        if progress is None:
            return

        state = "done" if progress.done else "working"
        self.query_one(".progress-label", Static).update(
            f"{progress.desc}  "
            f"{progress.completed}/{progress.total} {progress.unit}  {state}"
        )
        bar = self.query_one(ProgressBar)
        bar.update(total=max(progress.total, 1), progress=progress.completed)


class SwtopApp(App):
    """A live view of one `ds-service` server."""

    CSS = """
    Screen { layout: vertical; }

    #summary {
        padding: 0 1;
        color: $text-muted;
    }
    #error {
        padding: 0 1;
        color: $error;
    }

    #progress { height: auto; padding: 0 1; }
    .progress-label { color: $text-muted; }

    Block { height: auto; padding: 0 1; }
    /* Worker processes and tasks are the blocks that can hold thousands of
       rows, so they share the space left over and scroll inside themselves;
       the rest are bounded by what a cluster has. */
    #workers { height: 1fr; min-height: 6; }
    #tasks { height: 1fr; min-height: 6; }

    .block-title { text-style: bold; }
    .block-empty { color: $text-muted; }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "poll_now", "Refresh"),
    ]

    def __init__(self, collector: Collector, interval: float) -> None:
        super().__init__()
        self.collector = collector
        self.interval = interval
        self.snapshot: Snapshot | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="summary")
        yield Static(id="error")
        yield ProgressBlock(id="progress")
        yield Block(
            "worker jobs", WORKER_JOB_COLUMNS, EMPTY_WORKER_JOBS, id="worker-jobs"
        )
        yield Block("worker processes", WORKER_COLUMNS, EMPTY_WORKERS, id="workers")
        yield Block("hosts", HOST_COLUMNS, EMPTY_HOSTS, id="hosts")
        yield Block("slurm jobs", JOB_COLUMNS, EMPTY_JOBS, id="jobs")
        yield Block("tasks", TASK_COLUMNS, EMPTY_TASKS, id="tasks")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "swtop"
        self.sub_title = self.collector.address
        self.query_one("#error", Static).display = False
        self.query_one("#progress", ProgressBlock).display = False

        self.poll_now()
        self.set_interval(self.interval, self.poll_now)

    def action_poll_now(self) -> None:
        """Poll now rather than at the next interval."""
        self.poll_now()

    def poll_now(self) -> None:
        """Ask the server for a snapshot, without blocking the interface."""
        self.run_worker(
            self._poll,
            # One poll at a time.
            # Textual cancels the poll in flight.
            exclusive=True,
            group="poll",
        )

    async def _poll(self) -> None:
        """One poll, awaited in a worker so the interface does not block."""
        try:
            snapshot = await self.collector.snapshot()
        except Exception as e:
            # A server that is down, or not up yet, is worth waiting out.
            # This is a monitor.
            # If it quits, the screen goes with it.
            snapshot = Snapshot(
                address=self.collector.address,
                when=datetime.now(),
                error=f"{type(e).__name__}: {e}",
            )

        self.apply(snapshot)

    def apply(self, snapshot: Snapshot) -> None:
        """Draw one snapshot.

        Runs on the event loop, like every update.
        """
        self.snapshot = snapshot

        error = self.query_one("#error", Static)
        error.display = snapshot.error is not None
        if snapshot.error is not None:
            error.update(f"cannot read the server: {snapshot.error}")
            # The tables still show the last reading.
            return

        when = snapshot.when.strftime("%H:%M:%S")
        self.query_one("#summary", Static).update(f"{counts_line(snapshot)}   {when}")

        self.query_one("#progress", ProgressBlock).show(snapshot.progress)
        self.query_one("#worker-jobs", Block).show(worker_job_rows(snapshot))
        self.query_one("#workers", Block).show(worker_rows(snapshot))
        self.query_one("#hosts", Block).show(host_rows(snapshot))
        self.query_one("#jobs", Block).show(job_rows(snapshot))
        self.query_one("#tasks", Block).show(task_rows(snapshot))


async def run_app(collector: Collector, interval: float) -> None:
    """Run the terminal UI until the viewer quits.

    Await this on the loop the collector's client belongs to.
    """
    await SwtopApp(collector, interval).run_async()
