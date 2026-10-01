# `map`

[<- back to the main README](../../README.md)

`SlurmPilotExecutor.map` maps an iterable across the pool
and returns one value per item, in the order of the iterable.
The call blocks until every map task is back.
It hands out the items the way [`mapreduce`](mapreduce.md) does,
and leaves out the fold.

For when to use `map` and when to use `mapreduce`, see
[How to fold results across workers](../how-to-guides/fold-results-across-workers.md#keep-every-value-with-map).

```python
summaries = executor.map(
    desc="summarizing",
    queue="cpu",
    map_fn=summarize,
    iterable=paths,
    num_tasks=40,
)
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `desc` | required | Labels the progress `swtop` draws for this call. |
| `queue` | required | A job group name, or a list of them, as in `submit`. |
| `map_fn` | required | Runs once per item, on a worker. A callable, or the name of a method on the job group's actor. |
| `iterable` | required | The items. Read out in full before any map task starts. |
| `num_tasks` | required | How many map tasks drain the item queue, as an upper bound. |
| `map_extra_args`, `map_extra_kwargs` | `None` | Passed to `map_fn` after the item. |

The arguments are those of `mapreduce`
without `reduce_fn`, `init` and the two `reduce_extra_*` arguments.

Everything here travels by cloudpickle,
so `map_fn`, every item, every value
and every extra argument must be picklable.

Because `num_tasks` is an upper bound, two things follow.
A call with fewer items than map tasks submits one map task per item.
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
scores = executor.map(
    desc="scoring",
    queue="gpu",
    map_fn="predict",
    iterable=batches,
    num_tasks=16,
)
```

The rules are those of `mapreduce`.
Each map task looks the name up once,
on the actor its worker built at startup.
`map_extra_args` and `map_extra_kwargs` reach the method after the item.
A `queue` whose job group has no actor raises `ValueError`.
See [Mapping with an actor's method](mapreduce.md#mapping-with-an-actors-method).

## The queue it creates

A call creates an item queue named `<executor-name>.map.<n>.<token>`.
`<n>` counts the `map` calls on this executor that enqueued items.
It counts apart from `mapreduce`,
and the `.map.` segment keeps the two kinds of item queue apart.
`<token>` is 8 hex characters of a UUID4.
No job group serves the item queue.
Only that call's own map tasks claim from it.

Each item becomes an item task on it, `<item-queue>.item.<i>`.
That item task holds the pickled item and no function.
A map task marks the item task finished once it maps the item,
and the output it records is empty.
The value travels home inside the map task that computed it.
`ds-service` has no way to delete a task,
so those item tasks stay on the server for the life of the run.

The map tasks are ordinary tasks on `queue`,
named `<item-queue>.task.<i>`.

How a map task claims items, and the client it opens to claim them,
are the same as for `mapreduce`.
See [The queue it creates](mapreduce.md#the-queue-it-creates).

## What it costs

The driver enqueues each item with an RPC of its own, before any work starts.
The whole iterable is held in memory twice,
once on the driver and once on the server.
Every value is held twice as well.
The server holds it in the output of its map task,
and the driver holds it in the list the call returns.
`mapreduce` folds the values on the workers,
and keeps one value on the driver.
How to size an item against the cost of an RPC
is in [How to fold results across workers](../how-to-guides/fold-results-across-workers.md#chunk-the-items-when-each-one-is-small).

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
    This happens only when the invariant the call rests on is broken.

## Progress

The progress `swtop` draws counts the map tasks, not the items,
so the bar moves once per map task over the whole call.
`unit` is `task`.
`swtop`'s task table is the finer view,
where the item tasks complete one by one.

## Related

- [`SlurmPilotExecutor`](executor.md)
- [`mapreduce`](mapreduce.md)
- [How to fold results across workers](../how-to-guides/fold-results-across-workers.md)
