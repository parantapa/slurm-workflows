# Search spaces

[<- back to the main README](../../README.md)

`slurm_workflows.search_space`:
the parameter range types both space classes take,
together with the objective contract and the failure behavior
those two classes share.

A space is a mapping from **objective argument name** to a range.

```python
from slurm_workflows import IntRange, FloatRange, CategoricalRange

SPACE = {
    "layers": IntRange(1, 8),
    "learning_rate": FloatRange(1e-5, 1e-1, log_range=True),
    "optimizer": CategoricalRange(2),
}
```

| Range | The objective receives | Notes |
| --- | --- | --- |
| `IntRange(min, max)` | An `int` in `[min, max]` | |
| `FloatRange(min, max)` | A `float` in `[min, max]` | |
| `FloatRange(min, max, log_range=True)` | A `float` in `[min, max]` | Searched in log space, so each decade gets equal budget. Requires `min > 0`. |
| `CategoricalRange(n)` | An `int` in `[0, n - 1]` | An index into a caller-supplied list of values. `n = 1` is allowed but is a dead dimension. |

One space can mix all three.
A parameter the search must not vary
belongs in `extra_objective_kwargs` rather than in the space.

Both space classes map every parameter into `[0, 1]` before a model sees it,
and map it back for the objective.
The space rounds a continuous candidate
to get back an integer or a categorical parameter.
Both classes record where the objective actually ran, after rounding,
not the continuous candidate.

What that rounding costs a search on a mostly-discrete space is in
[Batch Bayesian optimization](../explanation/batch-bayesian-optimization.md).

## The objective

The same contract holds for both space classes.

The objective runs on a worker, once per point.
Its argument names must match the keys of `space`,
and it receives them as keyword arguments.
The executor cloudpickles it like any other task,
so a closure or a lambda is fine.
What it imports must exist on the compute node.

It returns a **mapping**, not a bare number.
The entry under `objective_key` is the objective value, and lower is better.
The objective must return the negative of a quantity to be maximized.
Both classes rank or model only that entry.
They record every other entry, which is where a runtime,
a checkpoint path or an unoptimized metric goes.

Both classes raise on a bare float, on a mapping without the key,
on a value that `float()` cannot convert,
or on a value that is not finite.
That covers `NaN` and `inf`,
because either one silently poisons a GP fit.

`extra_objective_kwargs` carries what the objective needs
but the search must not vary.
It must not shadow a key of `space`.
A shadowed key raises `ValueError` at construction.

## Failures

Both classes block until every pending point comes back.
A worker that raises does not raise on the driver,
so both classes wait with
[`RaiseOnError.RAISE_AFTER_COMPLETED`](executor.md#raiseonerror).
They turn what came back into a `RuntimeError` that names the studies that failed,
rather than feed a `RemoteExecutionError` into a model.

One bad evaluation therefore does not hide the rest of its batch.
Both classes record what did come back before they raise the exception.
`save()` therefore still holds the good points,
and the next run resumes from them.
