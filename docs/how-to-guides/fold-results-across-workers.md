# How to fold results across workers

[<- back to the main README](../../README.md)

Some work produces one number, not a result per task.
Counting the rows that match a filter, across ten thousand files, is one.
Totaling the events a whole set of runs recorded is another.
One task per item does the job, and it costs twice.
The driver holds every intermediate result,
and each item pays a task's overhead.

`mapreduce` does the summing on the workers instead.
It hands out the items and folds them on the worker that mapped them.
Each task brings back one partial result.

## Write a function that maps one item

It runs on a worker, once per item, and returns a value to fold:

```python
def count_hits(path, threshold):
    with open(path) as fobj:
        return sum(1 for line in fobj if float(line.split(",")[2]) > threshold)
```

## Call it

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

The call blocks until every task is back,
so scale the job group up before you call it.
Unlike `submit`, it cannot wait for workers that do not exist yet,
and raises `RuntimeError` instead.

## Pick a `reduce_fn` and an `init` that go together

Each task folds the items it claimed,
and the call folds the partial results those tasks return.
Both folds use the same function,
so **`reduce_fn` must be associative**, and **`init` must be its identity**.
For a count or a total, that is `operator.add` and `0`.

To gather values rather than total them,
map each item to a one-item list and concatenate:

```python
map_fn=lambda path: [summarize(path)], reduce_fn=add, init=[]
```

Appending instead of concatenating looks equivalent and is not.
`acc + [partial]` puts a whole partial result inside the answer.

For the wrong pairings worked through, see
[What `reduce_fn` and `init` must satisfy](../reference/mapreduce.md#what-reduce_fn-and-init-must-satisfy).

## Map with an actor's method

An expensive load belongs in an actor, once per worker.
On a job group you gave an `actor_class_name`,
give `map_fn` the name of one of its methods instead of a callable:

```python
map_fn="predict", reduce_fn=add, init=0
```

The model loads once per worker, whatever the number of items,
because each task resolves the name against the actor
its worker built at startup.
For the rules, and for what a job group without an actor raises, see
[Mapping with an actor's method](../reference/mapreduce.md#mapping-with-an-actors-method).

## Choose the two numbers separately

Set `num_tasks` to how many tasks you want draining the queue,
not to the number of items.
Size it by the pool, as you size any batch of tasks.
A few times the number of workers is a reasonable start.
A task that draws slow items then does not hold up the end of the run.

Nothing divides the items up in advance,
so each task takes the next item whenever it is free.

## Chunk the items when each one is small

Every item becomes a task on the server,
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

## Related

- [`mapreduce`](../reference/mapreduce.md)
- [How to keep per-worker state with actors](keep-per-worker-state-with-actors.md)
- [How to watch a run with `swtop`](watch-a-run-with-swtop.md)
