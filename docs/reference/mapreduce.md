# `mapreduce`

[<- back to the main README](../../README.md)

`SlurmPilotExecutor.mapreduce` maps an iterable across the pool
and folds what it produces into a single value.
The call blocks until every map task is back.

For the problem this solves and how to size a call, see
[How to fold results across workers](../how-to-guides/fold-results-across-workers.md).

```python
result = executor.mapreduce(
    desc="counting words",
    queue="cpu",
    map_fn=count_words,
    reduce_fn=operator.add,
    iterable=chunks,
    init=0,
    num_tasks=40,
)
```

`mapreduce` runs one function over a whole iterable
and folds what it produces into a single value.
It puts each item on a queue of its own.
Tasks on `queue` drain that queue,
and the call blocks until all of them are back.

Each task claims items one at a time and computes

```python
result = reduce_fn(
    result,
    map_fn(item, *map_extra_args, **map_extra_kwargs),
    *reduce_extra_args,
    **reduce_extra_kwargs,
)
```

starting from `init`,
until the queue holds nothing it can claim.
It returns that partial result.
The call then folds the partial results the same way,
and returns the value.

Nothing divides the items up in advance.
A task claims the next item whenever it is free,
so a slow item slows one task rather than a fixed share of the work.

| Argument | Meaning |
| --- | --- |
| `desc` | Labels the progress `swtop` draws for this call |
| `queue` | A job group name, or a list of them, as in `submit` |
| `map_fn` | Runs once per item, on a worker. A callable, or the name of a method on the job group's actor |
| `reduce_fn` | Folds one mapped value into the running result |
| `iterable` | The items. Read out in full before any task starts |
| `init` | Where every fold starts. Must be the identity of `reduce_fn` |
| `num_tasks` | How many tasks drain the queue, as an upper bound |
| `map_extra_args`, `map_extra_kwargs` | Passed to `map_fn` after the item |
| `reduce_extra_args`, `reduce_extra_kwargs` | Passed to `reduce_fn` after the two values |

Everything here travels by cloudpickle,
so `map_fn`, `reduce_fn`, `init`, every item
and every extra argument must be picklable.

## What `reduce_fn` and `init` must satisfy

**`reduce_fn` must be associative.**
It must also take a partial result as its second argument
as readily as a mapped one.
The final fold hands it two partial results.
Which items a task claimed depends on how busy the pool was,
so the grouping differs from one run to the next.

**`init` must be the identity of `reduce_fn`.**
Every task starts its fold at `init`, and so does the call.
`init` therefore enters the fold once per task, and once more at the end.

```python
# Right: sum, with 0.
map_fn=length, reduce_fn=operator.add, init=0

# Right: gather, where map_fn returns a list and reduce_fn concatenates.
map_fn=lambda x: [work(x)], reduce_fn=operator.add, init=[]

# Wrong: `init` is not an identity, so each task adds 1 of its own.
map_fn=length, reduce_fn=operator.add, init=1

# Wrong: appending a partial result nests it inside a list.
map_fn=work, reduce_fn=lambda acc, x: acc + [x], init=[]
```

The call folds into a copy of `init`,
so a `reduce_fn` that folds in place cannot write into the caller's value.

## Mapping with an actor's method

`map_fn` can be the name of a method
on the actor of the job group it runs on,
in place of a callable:

```python
executor.define_job_group(
    name="gpu",
    sbatch_args=[...],
    actor_class_name="my_pkg.model.Model",
)
executor.scale_jobs("gpu", 2)

score = executor.mapreduce(
    desc="scoring",
    queue="gpu",
    map_fn="predict",
    reduce_fn=operator.add,
    iterable=batches,
    init=0,
    num_tasks=16,
)
```

Each task looks the name up once,
on the actor its worker built at startup.
An expensive load therefore happens once per worker,
and not once per item.
`map_extra_args` and `map_extra_kwargs` reach the method after the item,
as they reach a callable.

`reduce_fn` is a callable, and takes no method name.
The final fold runs on the driver, where there is no actor.

A method name needs an actor to resolve against,
so a `queue` whose job group has none raises `ValueError`.
A callable `map_fn` runs on any job group,
with an actor or without one.
For the rest of what an actor does, see
[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md).

## The queue it creates

A call creates an item queue named `<executor-name>.mapreduce.<n>.<token>`.
`<n>` counts the calls on this executor,
and `<token>` is 8 hex characters of a UUID4.
No job group serves that queue.
Only that call's own tasks claim from it.
Each of them opens a `ds-service` client of its own,
from the `DS_SERVER_ADDRESS` the worker puts in the environment.
Each claims its items under `PILOT_WORKER_ID`,
so `task_get_worker_id` on an item names the worker that folded it.

Each item becomes a task on it, `<queue>.item.<i>`.
That task holds the pickled item and no function.
A task marks its item finished once it folds the value in,
and the output it records is empty.
The value travels home inside the task that computed it.
`ds-service` has no way to delete a task,
so those item tasks stay on the server for the life of the run.
They are what `swtop` counts,
and the `.mapreduce.` in the id is how a reader tells them apart.

The tasks that do the folding are ordinary tasks on `queue`,
named `<item-queue>.task.<i>`.

## What it costs

Enqueueing is one RPC per item, from the driver, before any work starts,
and there is no batched form of it.
The whole iterable is also held in memory twice,
once on the driver and once on the server.
Both say the same thing:
an item must carry enough work to be worth a round trip.
Combine small units into chunks and map over the chunks
when the work per item is smaller than the round trip that ships it.

## What it refuses

- A `num_tasks` below 1 raises `ValueError`.
- A `map_fn` given as a method name raises `ValueError`
    where a job group named in `queue` has no actor to find it on.
- A `queue` where no job group has a worker started
    raises `RuntimeError`, before it enqueues anything.
    Unlike `submit`, this call blocks,
    so it cannot wait for workers that do not exist yet.
- A task that fails raises `RuntimeError`, the way `wait` does.
    The other tasks keep draining the queue, and their results are discarded.

## Progress

The progress `swtop` draws counts the tasks, not the items,
so the bar moves `num_tasks` times over the whole call.
`unit` is `task`.
`swtop`'s task table is the finer view,
where the item tasks complete one by one.

Two more things follow from `num_tasks` being an upper bound.
A call with fewer items than tasks submits one task per item.
An empty `iterable` returns a copy of `init`,
creates no queue, submits nothing, and needs no worker.


## Related

- [`SlurmPilotExecutor`](executor.md)
- [How to fold results across workers](../how-to-guides/fold-results-across-workers.md)
