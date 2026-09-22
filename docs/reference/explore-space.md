# `ExploreSpaceSobolQMC`

[<- back to the main README](../../README.md)

`slurm_workflows.explore_space`:
the Sobol' quasi-Monte-Carlo exploration, its study dataclass,
and the results file both space classes write.

## `ExploreSpaceSobolQMC(studies, executor, num_exploration_points=None)`

```python
from slurm_workflows import ExplorationStudy, ExploreSpaceSobolQMC

exploration = ExploreSpaceSobolQMC(studies, executor, num_exploration_points=None)
```

Draws a Sobol' design over each space it is given,
evaluates every point of every design across the pool,
and keeps what came back.
`studies` is a **list** of `ExplorationStudy` objects, one per space.
`ExploreSpaceSobolQMC` submits them together,
so a small study does not wait for a large one.
`num_exploration_points` is the count for studies that do not carry their own.

It needs neither botorch nor torch, on the driver or on the workers.

## `ExplorationStudy`

| Field | Meaning |
| --- | --- |
| `name` | Names the study. Keys the results and labels its tasks on the queue. |
| `space` | The search space: one entry per objective argument. See [Search spaces](search-space.md). |
| `objective` | The function to evaluate. Its argument names must match the space's keys. |
| `objective_queue` | Queue, or queues, the evaluations go to. |
| `num_exploration_points` | Points to draw. Truncated down to a power of two. Optional if the exploration carries a default. |
| `seed` | Optional. The same seed redraws the same design. Without one, a seed is drawn and printed. |
| `objective_key` | Which entry of the objective's result is the value. `"objective"` by default. |
| `extra_objective_kwargs` | Extra arguments passed to the objective and not varied. |
| `priority` | The priority of every task the study submits. The highest priority runs first. `0.0` by default. |

The objective contract is the same for both classes:
see [The objective](search-space.md#the-objective).
What a failed evaluation does to a run is the same too:
see [Failures](search-space.md#failures).

## Methods

| Method | What it does |
| --- | --- |
| `design(name)` | The points a study will evaluate, without evaluating them. |
| `dim(name)` | How many dimensions a study's space has. |
| `run()` | Submit every point of every study and block until all are back. |
| `best_point(name)` | `(params, value)` of the lowest value the study measured. |
| `best_output(name)` | The objective's whole result at that point. |
| `save(path)` | Write the points, values and outputs to a gzipped pickle. |

```python
exploration = ExploreSpaceSobolQMC(
    [ExplorationStudy("demo", SPACE, objective, "cpu", 4096, seed=1)],
    executor,
)
exploration.run()
exploration.save("explore.pkl.gz")

result = exploration.results["demo"]   # points, values, outputs, unit_points
```

`exploration.results[name]` holds four index-aligned lists, in submission order.
They are the `points` evaluated, the `values` ranked,
the whole `outputs`, and `unit_points`, the points in the unit cube.
`exploration.studies` is the study list with the point count and seed filled in.
`ExploreSpaceSobolQMC` leaves the caller's own `ExplorationStudy` objects alone.

A second call to `run()` re-evaluates the same design:
the seed decides the draw, so there is no "next 4096 points".
A different seed draws a different design.

`ExploreSpaceSobolQMC` floors the point count to a power of two.
A Sobol' sequence is balanced at that prefix length.
64 workers that ask for 100 points evaluate 64 and leave the rest idle.

## The results file

`load_results(paths)` reads saved files back.
It merges them by study name
into a `dict[str, SavedResults]`.
Each file is a gzipped plain pickle, measurements rather than code.
The study name is the key, and each entry holds `points`, `values` and `outputs`
as index-aligned lists in submission order:

```python
import gzip, pickle

with gzip.open("explore.pkl.gz", "rb") as fobj:
    results = pickle.load(fobj)

results["demo"]["points"]     # the parameters of each evaluation
```

`unit_points` is not in the file: only the space can place a point
in the unit cube.
