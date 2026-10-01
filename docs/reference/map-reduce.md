# `map_reduce`

[<- back to the main README](../../README.md)

`SlurmPilotExecutor.map_reduce` maps an iterable across the pool
and folds what it produces into a single value.
The fold runs on the workers, and never on the driver.
The call blocks until the value is back.
`map` and `map_reduce` make up the simple interface of the executor.

[`map`](map.md) returns every value instead of one folded value.

```python
result = executor.map_reduce("cpu", count_words, operator.add, chunks, 0)
```

```python
SlurmPilotExecutor.map_reduce(
    queue, map_fn, reduce_fn, iterable, init, *,
    num_tasks=None, desc=None,
    map_extra_args=None, map_extra_kwargs=None,
    reduce_extra_args=None, reduce_extra_kwargs=None,
)
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `queue` | required | A job group name, or a list of them, as in [`submit`](submit-and-wait.md#submit-options). |
| `map_fn` | required | Runs once per item, on a worker. A callable, or the name of a method on the job group's actor. |
| `reduce_fn` | required | Folds one mapped value into the running result. |
| `iterable` | required | The items. Read out in full before any map task starts. |
| `init` | required | Where every fold starts. Must be the identity of `reduce_fn`. |
| `num_tasks` | `None` | How many map tasks drain the item queue, as an upper bound. `None` means `DEFAULT_MAP_TASKS`, which is 1024. |
| `desc` | `None` | Labels the progress `swtop` draws for this call. `None` means `map_reduce-<n>`. |
| `map_extra_args`, `map_extra_kwargs` | `None` | Passed to `map_fn` after the item. |
| `reduce_extra_args`, `reduce_extra_kwargs` | `None` | Passed to `reduce_fn` after the two values. |

The arguments after `init` are keyword-only.

In the default `desc`, `<n>` counts the `map_reduce` calls on this executor
that enqueued items, from 0.
It is the same `<n>` the item queue carries,
as [The queue it creates](#the-queue-it-creates) says.

Everything here travels by cloudpickle,
so `map_fn`, `reduce_fn`, `init`, every item
and every extra argument must be picklable.

Because `num_tasks` is an upper bound, two things follow.
A call with fewer items than map tasks submits one map task per item.
With the default, that means one map task per item, up to 1024.
A count above the number of workers costs little,
because a map task that starts after the item queue is empty
returns at once.

An empty `iterable` returns a copy of `init`,
creates no queue, submits nothing, and needs no worker.

## What the call computes

The call puts every item on an item queue of its own.
Map tasks on `queue` drain that item queue.

Each map task claims items one at a time and computes

```python
result = reduce_fn(
    result,
    map_fn(item, *map_extra_args, **map_extra_kwargs),
    *reduce_extra_args,
    **reduce_extra_kwargs,
)
```

with `result` set to `init` at the start,
until the queue holds nothing it can claim.
It returns that partial result.

The call also submits one reduce task on `queue`,
with every map task as its parent.
The server dispatches the reduce task only after every map task finishes.
The reduce task reads the partial results off the server one at a time,
folds them from `init` the same way,
and returns the value.
The call returns what the reduce task returned.
The partial results go from the workers to the server
and back to a worker.
They never reach the driver.

Nothing divides the items up in advance.
A map task claims the next item whenever it is free.
A slow item therefore slows one map task rather than a fixed share of the work.

## What `reduce_fn` and `init` must satisfy

**`reduce_fn` must be associative and commutative.**
It must also take a partial result as its second argument
as readily as a mapped one.
The reduce task hands it two partial results.
Which items a map task claimed depends on how busy the pool was,
so the grouping differs from one run to the next.
The reduce task folds the partial results in the order the call submitted the map tasks,
but which items each one holds still differs.

**`init` must be the identity of `reduce_fn`.**
Every map task starts its fold at `init`, and so does the reduce task.
`init` therefore enters the fold once per map task, and once more in the reduce task.

```python
# Right: sum, with 0.
map_fn=length, reduce_fn=operator.add, init=0

# Right: gather, where map_fn returns a list and reduce_fn concatenates.
# The items come back in no fixed order.
map_fn=lambda x: [work(x)], reduce_fn=operator.add, init=[]

# Wrong: `init` is not an identity, so each map task adds 1 of its own.
map_fn=length, reduce_fn=operator.add, init=1

# Wrong: appending a partial result nests it inside a list.
map_fn=work, reduce_fn=lambda acc, x: acc + [x], init=[]
```

Every task folds into its own copy of `init`,
so a `reduce_fn` that folds in place cannot write into the driver's `init`.

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

score = executor.map_reduce(
    "gpu", "predict", operator.add, batches, 0, desc="scoring"
)
```

Each map task looks the name up once,
on the actor its worker built at startup.
An expensive load therefore happens once per worker,
and not once per item.
`map_extra_args` and `map_extra_kwargs` reach the method after the item,
as they reach a callable.

`reduce_fn` is a callable, and takes no method name.

A method name needs an actor to resolve against,
so a `queue` whose job group has none raises `ValueError`.
A callable `map_fn` runs on any job group,
with an actor or without one.
For the rest of what an actor does, see
[How to keep per-worker state with actors](../how-to-guides/keep-per-worker-state-with-actors.md).

## The queue it creates

A call creates an item queue named `<executor-name>.map_reduce.<n>.<token>`.
`<n>` counts the `map_reduce` calls on this executor that enqueued items,
and `<token>` is 8 hex characters of a UUID4.
No job group serves that queue.
Only that call's own map tasks claim from it.

Each of them opens a `ds-service` client of its own,
from the `DS_SERVER_ADDRESS` the worker puts in the environment.
Each claims its items under `PILOT_WORKER_ID`,
so while an item task runs,
`task_get_worker_id` on it names the worker that folds its item.

Each item becomes an item task on the item queue, `<item-queue>.item.<i>`.
That item task holds the pickled item and no function.
A map task marks the item task finished once it folds the value in,
and the output it records is empty.
The value travels on inside the partial result of the map task that computed it.
`ds-service` has no way to delete a task,
so those item tasks stay on the server for the life of the run.
They are what `swtop` counts,
and the `.map_reduce.` in the id is how a reader tells them apart.

The map tasks are ordinary tasks on `queue`,
named `<item-queue>.task.<i>`.
The reduce task is an ordinary task on `queue` too,
named `<item-queue>.reduce`.
Its parents are the map tasks.

## What it costs

The driver enqueues each item with an RPC of its own, before any work starts.
No batched form of that RPC exists.
The whole iterable also sits in memory twice,
once on the driver and once on the server.

## What it refuses

- A `num_tasks` below 1 raises `ValueError`.
- A `map_fn` given as a method name raises `ValueError`
    where a job group named in `queue` has no actor to find it on.
- A `queue` where no job group has a pilot job submitted
    raises `RuntimeError`, before the call enqueues anything.
    A pilot job that is still pending is enough.
    An empty `iterable` returns before this check.
    Unlike `submit`, this call blocks,
    so it cannot wait for workers that do not exist yet.
- A map task that fails raises `RuntimeError`, the way `wait` does.
    The message carries the map task's own error.
    The server fails the reduce task too, without running it.
    The other map tasks still drain the item queue,
    and nothing reads their partial results.
- A reduce task that fails raises `RuntimeError` the same way.

## Progress

The progress `swtop` draws counts the map tasks and the reduce task, not the items.
So the bar moves once per map task, and once more at the end.
`unit` is `task`.
`swtop`'s task table is the finer view,
where the item tasks complete one by one.

## Related

- [`SlurmPilotExecutor`](executor.md)
- [`map`](map.md)
- [`submit`, `wait` and `as_completed`](submit-and-wait.md)
