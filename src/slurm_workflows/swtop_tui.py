"""The terminal UI `swtop` runs in, built with Textual.

The widgets come from `swtop_widgets.py`.
This module lays them out, and binds the keys.
"""

from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header

from .swtop import BLOCKS, Collector
from .swtop_widgets import (
    BlockTable,
    ErrorLine,
    ProgressDisplay,
    SnapshotPoller,
    SummaryLine,
    SwtopTabs,
)

# The key that shows each tab, by block key.
# Each is a letter of the block's title, and none is `q` or `r`.
# The keys are the app's, not the block's,
# so an app that embeds the tabs picks its own.
TAB_KEYS = {
    "pilot-jobs": "p",
    "workers": "w",
    "hosts": "h",
    "jobs": "j",
    "tasks": "t",
}


class SwtopApp(App):
    """A live view of one `ds-service` server."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "poll_now", "Refresh"),
        # `swtop-<key>` is the id `block_pane` gives each pane by default.
        *[
            Binding(TAB_KEYS[spec.key], f"show_tab('swtop-{spec.key}')", spec.title)
            for spec in BLOCKS
        ],
    ]

    def __init__(self, collector: Collector, interval: float) -> None:
        super().__init__()
        self.collector = collector
        self.interval = interval

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield SummaryLine(id="summary")
        yield SwtopTabs(id="blocks")
        yield ProgressDisplay(id="progress")
        yield ErrorLine(id="error")
        yield Footer()
        yield SnapshotPoller(
            collector=self.collector, interval=self.interval, id="poller"
        )

    def on_mount(self) -> None:
        self.title = "swtop"
        self.sub_title = self.collector.address
        # The poller mounts first, so its first poll can land before this.
        # `attach` shows that poll on the views, so none misses it.
        self.poller.attach(
            self.query_one(SummaryLine),
            self.query_one(ProgressDisplay),
            self.query_one(ErrorLine),
            *self.query(BlockTable),
        )

    @property
    def poller(self) -> SnapshotPoller:
        """The poller that fills the screen."""
        return self.query_one(SnapshotPoller)

    def action_poll_now(self) -> None:
        """Poll now rather than at the next interval."""
        self.poller.poll_now()

    def action_show_tab(self, pane_id: str) -> None:
        """Show the tab with id `pane_id`."""
        self.query_one(SwtopTabs).active = pane_id


async def run_app(collector: Collector, interval: float) -> None:
    """Run the terminal UI until the viewer quits.

    Await this on the loop the collector's client belongs to.
    """
    await SwtopApp(collector, interval).run_async()
