# `ExploreSpaceSobolQMC`

[<- back to the main README](../../README.md)

`slurm_workflows.explore_space`:
the Sobol' quasi-Monte-Carlo sweep, its task dataclass,
and the results file both space classes write.

```python
from slurm_workflows import ExplorationTask, ExploreSpaceSobolQMC

sweep = ExploreSpaceSobolQMC(tasks, executor, num_exploration_points=None)
```

Draws a Sobol' design over each space it is given,
evaluates every point of every design across the pool,
and keeps what came back.
`tasks` is a **list** of `ExplorationTask`s, one per space:
they are submitted together, so a small sweep does not wait for a large one.
`num_exploration_points` is the count for tasks that do not carry their own.

It needs neither botorch nor torch, on the driver or on the workers.

## `ExplorationTask`

| Field | Meaning |
| --- | --- |
| `name` | Names the task. Keys the results and labels the tasks on the queue. |
| `space` | The search space: one entry per objective argument. See [Search spaces](search-space.md). |
| `objective` | The function to evaluate. Its argument names must match the space's keys. |
| `objective_queue` | Queue, or queues, the evaluations go to. |
| `num_exploration_points` | Points to draw. Truncated down to a power of two. Optional if the sweep carries a default. |
| `seed` | Optional. The same seed redraws the same design; without one, a seed is drawn and printed. |
| `objective_key` | Which entry of the objective's result is the value. `"objective"` by default. |
| `extra_objective_kwargs` | Extra arguments passed to the objective and not varied. |

## The objective

The same contract holds for both space classes.

The objective runs on a worker, once per point.
Its argument names must match the keys of `space`,
and it receives them as keyword arguments.
It is cloudpickled like any other task,
so a closure or a lambda is fine,
as long as what it imports exists on the compute node.

It returns a **mapping**, not a bare number.
The entry under `objective_key` is the value,
lower being better, so negate a score you would rather maximize.
Only that entry is ranked or modelled;
every other entry is recorded, which is where a runtime,
a checkpoint path or an unoptimized metric goes.

A bare float, a mapping without the key,
or a value that is not a finite float
each raise rather than being coerced -
`NaN` and `inf` included, since either one silently poisons a GP fit.

`extra_objective_kwargs` carries what the objective needs
but the search must not vary.
It may not shadow a key of `space`, which is rejected when the run is built.

## Failures

Both classes block until every point in flight has come back.
A worker that raises does not raise on the driver,
so both wait with `RaiseOnError.RAISE_AFTER_COMPLETED`
and turn what came back into a `RuntimeError` naming the tasks that failed,
rather than feeding a `RemoteExecutionError` into a model.
One bad evaluation therefore does not hide the rest of its batch,
and what did come back is recorded before the exception is raised,
so `save()` still holds the good points and the next run resumes from them.

## Methods

| Method | What it does |
| --- | --- |
| `design(name)` | The points a task will evaluate, without evaluating them. |
| `dim(name)` | How many dimensions a task's space has. |
| `run()` | Submit every point of every task and block until all are back. |
| `best_point(name)` | `(params, value)` of the lowest value the task measured. |
| `best_output(name)` | The objective's whole result at that point. |
| `save(path)` | Write the points, values and outputs to a gzipped pickle. |

```python
sweep = ExploreSpaceSobolQMC(
    [ExplorationTask("sweep", SPACE, objective, "cpu", 4096, seed=1)],
    executor,
)
sweep.run()
sweep.save("sweep.pkl.gz")

result = sweep.results["sweep"]     # points, values, outputs, unit_points
```

`sweep.results[name]` holds four index-aligned lists,
in submission order:
the `points` evaluated, the `values` ranked,
the whole `outputs`, and `unit_points`, the points in the unit cube.
`sweep.tasks` is the task list with the point count and seed filled in;
the caller's own `ExplorationTask` objects are left alone.

Calling `run()` again re-evaluates the same design:
the seed decides the draw, so there is no "next 4096 points".
A different seed draws a different design.

The point count is floored to a power of two,
which is the prefix length at which a Sobol' sequence is balanced.
64 workers asking for 100 points evaluate 64 and leave the rest idle.

## The results file

Read a saved file back with `load_results(paths)`,
which merges several files by task name
into a `dict[str, SavedResults]`.
The file is a gzipped plain pickle - measurements, not code -
keyed by task name, each holding `points`, `values` and `outputs`
as index-aligned lists in submission order:

```python
import gzip, pickle

with gzip.open("sweep.pkl.gz", "rb") as fobj:
    results = pickle.load(fobj)

results["sweep"]["points"]     # the parameters of each evaluation
```

`unit_points` is not in the file: only the space can place a point
in the unit cube.
