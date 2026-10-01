# `map`

[<- back to the main README](../../README.md)

`SlurmPilotExecutor.map` maps an iterable across the pool
and returns one value per item, in the order of the iterable.
The call blocks until every map task is back.
It hands out the items the way [`map_reduce`](map-reduce.md) does,
and leaves out the fold.
`map` and `map_reduce` make up the simple interface of the executor.

```python
summaries = executor.map("cpu", summarize, paths)
```

```python
SlurmPilotExecutor.map(
    queue, map_fn, iterable, *,
    num_tasks=None, desc=None,
    map_extra_args=None, map_extra_kwargs=None,
) -> list
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `queue` | required | A job group name, or a list of them, as in [`submit`](submit-and-wait.md#submit-options). |
| `map_fn` | required | Runs once per item, on a worker. A callable, or the name of a method on the job group's actor. |
| `iterable` | required | The items. Read out in full before any map task starts. |
| `num_tasks` | `None` | How many map tasks drain the item queue, as an upper bound. `None` means `DEFAULT_MAP_TASKS`, which is 1024. |
| `desc` | `None` | Labels the progress `swtop` draws for this call. `None` means `map-<n>`. |
| `map_extra_args`, `map_extra_kwargs` | `None` | Passed to `map_fn` after the item. |

The arguments after `iterable` are keyword-only.
The arguments are those of `map_reduce`
without `reduce_fn`, `init` and the two `reduce_extra_*` arguments.

In the default `desc`, `<n>` counts the `map` calls on this executor
that enqueued items, from 0.
It is the same `<n>` the item queue carries,
as [The queue it creates](#the-queue-it-creates) says.

Everything here travels by cloudpickle,
so `map_fn`, every item, every value
and every extra argument must be picklable.

Because `num_tasks` is an upper bound, two things follow.
A call with fewer items than map tasks submits one map task per item.
With the default, that means one map task per item, up to 1024.
A count above the number of workers costs little,
because a map task that starts after the item queue is empty
returns at once.

An empty `iterable` returns `[]`,
creates no queue, submits nothing, and needs no worker.

## What the call computes

The call puts every item on an item queue of its own.
Map tasks on `queue` drain that item queue.

Each map task claims items one at a time and computes

```python
map_fn(item, *map_extra_args, **map_extra_kwargs)
```

until the queue holds nothing it can claim.
It returns each value it computed,
paired with the id of the item task the value came from.
The call places each value at the index of its item,
and returns the list.

The list is in the order of `iterable`.
Which map task claimed an item, and when that task finished,
makes no difference to where the value lands.

Nothing divides the items up in advance.
A map task claims the next item whenever it is free.
A slow item therefore slows one map task rather than a fixed share of the work.

## Mapping with an actor's method

`map_fn` can be the name of a method
on the actor of the job group it runs on,
in place of a callable:

```python
scores = executor.map("gpu", "predict", batches, desc="scoring")
```

The rules are those of `map_reduce`.
Each map task looks the name up once,
on the actor its worker built at startup.
`map_extra_args` and `map_extra_kwargs` reach the method after the item.
A `queue` whose job group has no actor raises `ValueError`.
See [Mapping with an actor's method](map-reduce.md#mapping-with-an-actors-method).

## The queue it creates

A call creates an item queue named `<executor-name>.map.<n>.<token>`.
`<n>` counts the `map` calls on this executor that enqueued items.
It counts apart from `map_reduce`,
and the `.map.` segment keeps the two kinds of item queue apart.
`<token>` is 8 hex characters of a UUID4.
No job group serves the item queue.
Only that call's own map tasks claim from it.

Each item becomes an item task on the item queue, `<item-queue>.item.<i>`.
That item task holds the pickled item and no function.
A map task marks the item task finished once it maps the item,
and the output it records is empty.
The value travels home inside the map task that computed it.
`ds-service` has no way to delete a task,
so those item tasks stay on the server for the life of the run.

The map tasks are ordinary tasks on `queue`,
named `<item-queue>.task.<i>`.

How a map task claims items, and the client it opens to claim them,
are the same as for `map_reduce`.
See [The queue it creates](map-reduce.md#the-queue-it-creates).

## What it costs

The driver enqueues each item with an RPC of its own, before any work starts.
The whole iterable sits in memory twice,
once on the driver and once on the server.
Every value sits in memory twice as well.
The server holds it in the output of its map task,
and the driver holds it in the list the call returns.
`map_reduce` folds the values on the workers,
and keeps one value on the driver.

## What it refuses

- A `num_tasks` below 1 raises `ValueError`.
- A `map_fn` given as a method name raises `ValueError`
    where a job group named in `queue` has no actor to find it on.
- A `queue` where no job group has a pilot job submitted
    raises `RuntimeError`, before the call enqueues anything.
    A pilot job that is still pending is enough.
    An empty `iterable` returns before this check.
- A map task that fails raises `RuntimeError`, the way `wait` does.
    The call returns no values in that case,
    not even those of the map tasks that finished.
- An item that comes back with no value raises `RuntimeError`.
    This happens only when the invariant the call rests on does not hold.

## Progress

The progress `swtop` draws counts the map tasks, not the items,
so the bar moves once per map task over the whole call.
`unit` is `task`.
`swtop`'s task table is the finer view,
where the item tasks complete one by one.

## Related

- [`SlurmPilotExecutor`](executor.md)
- [`map_reduce`](map-reduce.md)
- [`submit`, `wait` and `as_completed`](submit-and-wait.md)
