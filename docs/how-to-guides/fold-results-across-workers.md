# How to fold results across workers

[<- back to the main README](../../README.md)

Some work produces one number, not a result per task.
Counting the rows that match a filter, across ten thousand files, is one.
Totaling the events a whole set of runs recorded is another.
One task per item does the job, and it costs twice.
The coordinator holds every intermediate result,
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

## Pick a `reduce_fn` and an `init` that go together

`reduce_fn` folds one value into the running result.
Each task folds the items it claimed,
and the call folds the partial results the tasks return.
Both folds use the same function, so **`reduce_fn` must be associative**,
and **`init` must be its identity**.

For a count or a total, that is `operator.add` and `0`:

```python
from operator import add
```

To gather values rather than total them,
map each item to a one-item list and concatenate:

```python
map_fn=lambda path: [summarize(path)], reduce_fn=add, init=[]
```

Appending instead of concatenating looks equivalent and is not.
The final fold hands `reduce_fn` a partial result,
and `acc + [partial]` puts a whole list inside the answer.

## Call it

```python
with SlurmPilotExecutor("scan", address) as executor:
    executor.define_worker(name="cpu", sbatch_args=SBATCH_ARGS)
    executor.scale_workers("cpu", 4)

    hits = executor.mapreduce(
        description="scanning",
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

The call blocks until every task is back.
Scale the workers up first.
Unlike `submit`, this call cannot wait for workers that do not exist yet.
It raises `RuntimeError` instead.

## Choose the two numbers separately

`num_tasks` is how many tasks drain the queue, not how many items there are.
Size it by the pool, as you size any batch of tasks.
A few times the number of worker processes is a reasonable start.
A task that draws slow items then does not hold up the end of the run.

Nothing divides the items up in advance.
Each task takes the next item whenever it is free.
The pool therefore absorbs an uneven item, rather than one task's share.

## Chunk the items when each one is small

Every item becomes a task on the queue server,
which costs one round trip to put there.
Work that takes less time than that round trip
belongs in groups:

```python
def count_hits_in_chunk(paths, threshold):
    return sum(count_hits(path, threshold) for path in paths)


chunks = [paths[i : i + 50] for i in range(0, len(paths), 50)]
```

Then map over `chunks` rather than over `paths`.
The fold is unchanged, because the chunk's count folds like a file's count.

## Watch it

`description` labels the progress `swtop` draws.
That bar counts the tasks, not the items,
so it moves a few dozen times over a run of ten thousand files.
The items appear in `swtop`'s task table instead,
under ids that carry `.mapreduce.`.
They complete one by one while the bar sits still.

## What it will not do

- **Run on a worker group with an actor.**
    A worker with an actor looks its function up by name on that actor,
    and this call sends a callable.
    It raises `ValueError` rather than failing on the node.
- **Return a partial answer.**
    A task that fails raises `RuntimeError`, as `wait` does.
    A fold missing a shard of its input is not worth returning.

## Related

- [`mapreduce`](../reference/executor.md#mapreduce)
- [How to keep per-worker state with actors](keep-per-worker-state-with-actors.md)
- [How to watch a run with `swtop`](watch-a-run-with-swtop.md)
