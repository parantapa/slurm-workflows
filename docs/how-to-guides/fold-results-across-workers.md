# How to fold results across workers

[<- back to the main README](../../README.md)

Sometimes your work produces one number, not one task output per item.
For example, it counts the rows that match a filter across ten thousand files,
or totals the events that a whole set of runs recorded.
In that case, use `mapreduce` rather than one task per item.
One task per item makes the driver hold every task output,
and makes each item pay a task's overhead.
`mapreduce` hands out the items and folds them on the worker that mapped them,
so each map task brings back one partial result.

If you need one value per item instead,
in the order of the items,
see [Keep every value with `map`](#keep-every-value-with-map).

## Write a function that maps one item

The function runs on a worker, once per item, and returns a value to fold:

```python
def count_hits(path, threshold):
    with open(path) as fobj:
        return sum(1 for line in fobj if float(line.split(",")[2]) > threshold)
```

## Call `mapreduce`

```python
from operator import add

with SlurmPilotExecutor("scan", address) as executor:
    executor.define_job_group(name="cpu", sbatch_args=SBATCH_ARGS)
    executor.scale_jobs("cpu", 4)

    hits = executor.mapreduce(
        desc="scanning",
        queue="cpu",
        map_fn=count_hits,
        reduce_fn=add,
        iterable=paths,
        init=0,
        num_tasks=40,
        map_extra_args=(1.5,),
    )

print(hits)
```

`map_extra_args` and `map_extra_kwargs` reach `map_fn` after the item,
which is how `threshold` gets there.
`reduce_extra_args` and `reduce_extra_kwargs` do the same for `reduce_fn`.

The call blocks until every map task is back,
so scale the job group up before you call it.
If no job group named in `queue` has a pilot job, the call raises `RuntimeError`.
See [What it refuses](../reference/mapreduce.md#what-it-refuses).

## Pick a `reduce_fn` and an `init` that go together

Each map task folds the items it claimed,
and the call folds the partial results those map tasks return.
Both folds use the same function,
so `reduce_fn` must be associative, and `init` must be its identity.
The partial results come back in the order the map tasks finish.
So `reduce_fn` must also be commutative
to give the same answer on every run.
For a count or a total, that is `operator.add` and `0`.

To gather values rather than total them,
map each item to a one-item list and concatenate:

```python
map_fn=lambda path: [summarize(path)], reduce_fn=add, init=[]
```

Concatenation is not commutative.
The list holds every value, but in no fixed order.
To get the values in the order of the items, use `map` instead.
See [Keep every value with `map`](#keep-every-value-with-map).

An append in place of a concatenation looks equivalent, and is not.
`acc + [partial]` puts a whole partial result inside the answer.

For the wrong pairings worked through, see
[What `reduce_fn` and `init` must satisfy](../reference/mapreduce.md#what-reduce_fn-and-init-must-satisfy).

## Map with an actor's method

An expensive load belongs in an actor, once per worker.
On a job group you gave an `actor_class_name`,
give `map_fn` an actor method name instead of a callable:

```python
map_fn="predict", reduce_fn=add, init=0
```

The actor's expensive load runs once per worker, whatever the number of items.
This happens because each map task resolves the name against the actor
that its worker built at startup.
For the rules, and for what a job group without an actor raises, see
[Mapping with an actor's method](../reference/mapreduce.md#mapping-with-an-actors-method).

## Keep every value with `map`

When you need one value per item, and not one folded value,
call `map` in place of `mapreduce`.
It takes the same arguments,
less `reduce_fn`, `init` and the two `reduce_extra_*` arguments,
and it returns a list in the order of the items:

```python
summaries = executor.map(
    desc="summarizing",
    queue="cpu",
    map_fn=summarize,
    iterable=paths,
    num_tasks=40,
)
```

`map` hands out the items the same way,
so the sections on an actor's method, on `num_tasks`
and on chunking apply to it as well.
The difference is memory:
the driver holds every value at the end of the call.
For a count or a total, `mapreduce` keeps that list off the driver.
For the details, see [`map`](../reference/map.md).

## Set `num_tasks` by the pool, not by the item count

Nothing divides the items in advance,
so each map task claims the next item whenever it is free.

Set `num_tasks` to the number of map tasks that drain the item queue,
not to the number of items.
Size it by the pool, as you size any batch of tasks.
A few times the number of workers is a reasonable start.
A map task that claims slow items then does not delay the end of the run.

## Chunk the items when each one is small

Every item becomes an item task on the server,
which costs one round trip to put there.
Work that takes less time than that round trip
belongs in chunks:

```python
def count_hits_in_chunk(paths, threshold):
    return sum(count_hits(path, threshold) for path in paths)


chunks = [paths[i : i + 50] for i in range(0, len(paths), 50)]
```

Then map over `chunks` rather than over `paths`.
The fold is unchanged, because the chunk's count folds like a file's count.

With `map`, each value is then the result of one chunk.
To get one value per item,
have the chunk function return a list, and flatten the result.

## Related

- [`mapreduce`](../reference/mapreduce.md)
- [`map`](../reference/map.md)
- [How to keep per-worker state with actors](keep-per-worker-state-with-actors.md)
- [How to watch a run with `swtop`](watch-a-run-with-swtop.md)
