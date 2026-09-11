# `ExploreSpaceSobolQMC`

[<- back to the main README](../../README.md)

`slurm_workflows.explore_space`:
the Sobol' quasi-Monte-Carlo sweep, its task dataclass,
and the results file both space classes write.

```python
from slurm_workflows import ExplorationTask, ExploreSpaceSobolQMC

sweep = ExploreSpaceSobolQMC(tasks, executor, num_exploration_points=None)
```

Draws a Sobol' design over each space you give it,
evaluates every point of every design across the pool,
and keeps what came back.
`tasks` is a **list** of `ExplorationTask`s, one per space.
`ExploreSpaceSobolQMC` submits them together,
so a small sweep does not wait for a large one.
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
The executor cloudpickles it like any other task,
so a closure or a lambda is fine.
What it imports must exist on the compute node.

It returns a **mapping**, not a bare number.
The entry under `objective_key` is the value, and lower is better.
Negate a score you want to maximize.
Both classes rank or model only that entry.
They record every other entry, which is where a runtime,
a checkpoint path or an unoptimized metric goes.

Both classes raise on a bare float, on a mapping without the key,
or on a value that is not a finite float.
They never coerce one.
That covers `NaN` and `inf`,
because either one silently poisons a GP fit.

`extra_objective_kwargs` carries what the objective needs
but the search must not vary.
It must not shadow a key of `space`.
A shadowed key raises `ValueError` when you build the run.

## Failures

Both classes block until every point in flight comes back.
A worker that raises does not raise on the driver,
so both classes wait with `RaiseOnError.RAISE_AFTER_COMPLETED`.
They turn what came back into a `RuntimeError` that names the tasks that failed,
rather than feed a `RemoteExecutionError` into a model.

One bad evaluation therefore does not hide the rest of its batch.
Both classes record what did come back before they raise the exception.
`save()` therefore still holds the good points,
and the next run resumes from them.

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

`sweep.results[name]` holds four index-aligned lists, in submission order.
They are the `points` evaluated, the `values` ranked,
the whole `outputs`, and `unit_points`, the points in the unit cube.
`sweep.tasks` is the task list with the point count and seed filled in.
`ExploreSpaceSobolQMC` leaves the caller's own `ExplorationTask` objects alone.

A second call to `run()` re-evaluates the same design:
the seed decides the draw, so there is no "next 4096 points".
A different seed draws a different design.

`ExploreSpaceSobolQMC` floors the point count to a power of two.
A Sobol' sequence is balanced at that prefix length.
64 workers that ask for 100 points evaluate 64 and leave the rest idle.

## The results file

Read a saved file back with `load_results(paths)`,
which merges several files by task name
into a `dict[str, SavedResults]`.
The file is a gzipped plain pickle, measurements rather than code.
The task name is the key, and each entry holds `points`, `values` and `outputs`
as index-aligned lists in submission order:

```python
import gzip, pickle

with gzip.open("sweep.pkl.gz", "rb") as fobj:
    results = pickle.load(fobj)

results["sweep"]["points"]     # the parameters of each evaluation
```

`unit_points` is not in the file: only the space can place a point
in the unit cube.
