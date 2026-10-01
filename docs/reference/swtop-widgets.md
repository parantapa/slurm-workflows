# `slurm_workflows.swtop_widgets`

[<- back to the main README](../../README.md)

`slurm_workflows.swtop_widgets` holds the widgets
that make the terminal UI of [`swtop`](swtop.md).
Another Textual app can lay them out in its own screen.
To do that, see
[How to embed `swtop` in a Textual app](../how-to-guides/embed-swtop-in-a-textual-app.md).

| Name | What it is |
| --- | --- |
| `SnapshotPoller` | Polls one server, and calls `show` on each view attached to it. It draws nothing. |
| `SummaryLine` | The summary line. |
| `ProgressDisplay` | The progress bar. Hidden until a driver waits on something. |
| `ErrorLine` | The error line. Hidden while polls succeed. |
| `BlockTable(spec, **kwargs)` | The table of one block, or why the block is empty. It has no title. |
| `TaskTable(spec, states=DEFAULT_TASK_STATES, **kwargs)` | A `BlockTable` for the tasks block, with a `TaskStateFilter` above the rows. It shows only the tasks in `states`. |
| `TaskStateFilter(states=DEFAULT_TASK_STATES, **kwargs)` | A checkbox for each task state. It posts `TaskStateFilter.Changed`, and `event.states` is the set of states now checked. |
| `block_table(spec, **kwargs)` | A `TaskTable` for the tasks block, and a `BlockTable` for any other block. |
| `block_pane(spec, *, id=None)` | A `TabPane` that holds the `block_table` of `spec`, and keeps its count in the tab label. The id is `swtop-<key>` by default. |
| `SwtopTabs` | A `TabbedContent` with one `block_pane` for each block. |
| `BLOCKS` | In `slurm_workflows.swtop`: one `BlockSpec` for each block, in screen order. Its `key` is `pilot-jobs`, `workers`, `hosts`, `jobs` or `tasks`. |
| `DEFAULT_TASK_STATES` | In `slurm_workflows.swtop`: the states a `TaskTable` shows at start, `Waiting`, `Ready` and `Running`. |

Any `**kwargs`, such as `id`, goes on to the Textual widget.
`block_table` passes it to the table it builds.

## `SnapshotPoller`

`SnapshotPoller` takes exactly one of these two arguments:

- `address`: the poller opens a client of its own when it mounts,
  and closes it when it unmounts.
- `collector`: a `Collector` that the caller owns.
  Its client must belong to the app's event loop.
  `open_collector(address)` in `slurm_workflows.swtop`
  is an async context manager that yields one
  on a client of its own, and closes that client when the `async with` block exits.
  The `async with` that enters it must run on the app's event loop.

Its other arguments are `interval` and `views`.
The `interval` is in seconds, and `2.0` by default.
The `views` are the views to attach from the start.
It raises `ValueError` when it gets both `address` and `collector` or neither,
or when `interval` is not greater than 0.
Every argument is keyword-only, as in `SnapshotPoller(address="host:5051")`.
Any other keyword argument, such as `id`, goes to the Textual `Widget`.

| Member | What it does |
| --- | --- |
| `attach(*views)` | Shows each later poll on `views`. A view attached after a poll shows that poll at once. A widget must be mounted before it is attached. |
| `detach(*views)` | Stops showing polls on `views`. |
| `poll_now()` | Polls now, rather than at the next interval. |
| `snapshot` | The last `Snapshot`, or `None` before the first poll ends. |
| `Polled` | The message posted after each poll. `event.snapshot` is the `Snapshot`, with `error` set if the poll failed. |

## Messages, keys and styles

`BlockTable` posts `BlockTable.CountChanged` when its row count changes.
`event.spec` is its block, and `event.count` is the new count.
A `TaskTable` counts only the tasks it shows.

A failed poll changes only the error line.
The other widgets keep the last good reading.

The widgets add no key bindings of their own.
The tabs and the tables inside them keep the usual Textual keys,
such as the arrow keys, while they have focus.
The only ids the widgets set are the `swtop-<key>` ids of the block panes.
The app that lays them out picks every other key binding and every other id.
For example, it binds a key to each tab and a key to `poll_now`.

Each widget carries its own styles,
so it needs no CSS from the app.

## Related

- [`swtop`](swtop.md)
- [How to embed `swtop` in a Textual app](../how-to-guides/embed-swtop-in-a-textual-app.md)
