# How to embed `swtop` in a Textual app

[<- back to the main README](../../README.md)

Your own Textual app can show what `swtop` shows,
next to views of its own.
You can put any of the `swtop` tabs among your own tabs,
and put the summary line, the progress bar and the error line
anywhere in your layout.

For every widget and its arguments, see
[`swtop` reference](../reference/swtop.md#slurm_workflowsswtop_widgets).

## Lay out the widgets

Compose the widgets you want, and one `SnapshotPoller` for the server.
Then attach the widgets to the poller in `on_mount`:

```python
from textual.app import App, ComposeResult
from textual.widgets import Static, TabbedContent, TabPane

from slurm_workflows.swtop import BLOCKS
from slurm_workflows.swtop_widgets import (
    BlockTable,
    ErrorLine,
    ProgressDisplay,
    SnapshotPoller,
    SummaryLine,
    block_pane,
)


class MyApp(App):
    def compose(self) -> ComposeResult:
        yield SummaryLine()
        with TabbedContent():
            with TabPane("My results", id="results"):
                yield Static("my own view", id="my-status")
            for spec in BLOCKS:
                if spec.key in {"workers", "tasks"}:
                    yield block_pane(spec)
        yield ProgressDisplay()
        yield ErrorLine()
        yield SnapshotPoller(address="10.0.0.1:5051", interval=2.0)

    def on_mount(self) -> None:
        self.query_one(SnapshotPoller).attach(
            self.query_one(SummaryLine),
            self.query_one(ProgressDisplay),
            self.query_one(ErrorLine),
            *self.query(BlockTable),
        )


MyApp().run()
```

The poller opens its own client on the app's event loop,
and closes it when the app exits.
Each tab keeps its row count in its label,
in your `TabbedContent` as in `swtop`.

To take all five tabs as they are,
compose `SwtopTabs()` in place of your own `TabbedContent`.

## Bind your own keys

The `swtop` widgets add no key bindings of their own,
so the letter keys stay free for your app.
The tabs and the tables inside them keep the usual Textual keys,
such as the arrow keys, while they have focus.
Bind the keys you want in your app:

```python
class MyApp(App):
    BINDINGS = [
        ("r", "refresh_swtop", "Refresh"),
        ("w", "show_tab('swtop-workers')", "Workers"),
        ("t", "show_tab('swtop-tasks')", "Tasks"),
    ]

    def action_refresh_swtop(self) -> None:
        self.query_one(SnapshotPoller).poll_now()

    def action_show_tab(self, pane_id: str) -> None:
        self.query_one(TabbedContent).active = pane_id
```

A tab from `block_pane` has the id `swtop-` and the block key by default,
such as `swtop-workers`.
Pass `id=` to `block_pane` to choose another.

## React to a poll in your own widgets

The poller posts `SnapshotPoller.Polled` after each poll.
Handle it to use the reading in a widget of your own:

```python
    def on_snapshot_poller_polled(self, event: SnapshotPoller.Polled) -> None:
        if event.snapshot.error is None:
            running = event.snapshot.counts.get("running", 0)
            self.query_one("#my-status", Static).update(f"{running} running")
```

A failed poll sets `event.snapshot.error`,
and leaves the rest of the snapshot empty.

## Watch two servers

Give each server its own poller, and attach each view to one poller:

```python
    def compose(self) -> ComposeResult:
        workers = next(spec for spec in BLOCKS if spec.key == "workers")
        yield BlockTable(workers, id="a-workers")
        yield BlockTable(workers, id="b-workers")
        yield SnapshotPoller(address="10.0.0.1:5051", id="a")
        yield SnapshotPoller(address="10.0.0.2:5051", id="b")

    def on_mount(self) -> None:
        self.query_one("#a", SnapshotPoller).attach(self.query_one("#a-workers", BlockTable))
        self.query_one("#b", SnapshotPoller).attach(self.query_one("#b-workers", BlockTable))
```

The two pollers poll on their own, and do not cancel each other.
Two `block_pane` tabs of the same block need different ids.

## Related

- [How to watch a run with `swtop`](watch-a-run-with-swtop.md)
