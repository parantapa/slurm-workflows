# The objective

[<- back to the main README](../../README.md)

The contract an objective function meets,
and what a failed evaluation does to a run.
The same contract holds for `ExploreSpaceSobolQMC` and `OptimizeSpaceBotorch`.
The ranges its arguments come from
are in [Search spaces](search-space.md).

## The contract

The objective runs on a worker, once per point.
Its argument names must match the keys of `space`,
and it receives them as keyword arguments.
The executor serializes it with cloudpickle like any other task,
so it can be a closure or a lambda.
What it imports must exist on the compute node.

It returns a mapping, not a bare number.
The entry under `objective_key` is the objective value, and lower is better.
An objective that maximizes a quantity must return its negative.
Both classes rank or model only that entry.
They record every other entry, which is where a runtime,
a checkpoint path or an unoptimized metric goes.

Both classes raise on a bare float, or on a mapping without the key.
They also raise on a value that `float()` cannot convert,
or on a value that is not finite.
This last case covers `NaN` and `inf`,
because either one silently poisons a Gaussian process fit.

`extra_objective_kwargs` carries what the objective needs
but the search must not vary.
It must not shadow a key of `space`.
A shadowed key raises `ValueError` at construction.

For a `space` with the keys `x` and `y`,
an `extra_objective_kwargs` of `{"scale": 2.0}`
and the default `objective_key` of `"objective"`,
this objective meets the contract:

```python
import time


def objective(x: float, y: int, scale: float) -> dict[str, float]:
    start = time.monotonic()
    value = scale * (x - 1.0) ** 2 + y
    return {"objective": value, "runtime": time.monotonic() - start}
```

Both classes rank or model the `"objective"` entry,
and record `runtime` beside it.

## Failures

Both classes block until every pending point comes back.
A worker that raises does not raise on the driver,
so both classes wait
with [`RaiseOnError.RAISE_AFTER_COMPLETED`](executor.md#raiseonerror).
They turn what came back into a `RuntimeError` that names the studies that failed,
rather than feed a `RemoteExecutionError` into a model.

One bad evaluation therefore does not hide the rest of its batch.
Both classes record what did come back before they raise the exception.
`save()` therefore still holds the good points,
and an `OptimizeSpaceBotorch` run can start from them
(see [How to resume a search](../how-to-guides/resume-a-search.md)).

This holds for an objective that raises.
In a batch where no objective raised,
an objective can return a result the contract rejects,
such as a bare float or a `NaN`.
Its class then raises as soon as it records that result,
and does not record the points after it in submission order.
In a batch where an objective also raised,
the class skips the rejected point,
records the rest,
and raises for the failed batch.

## Related

- [Search spaces](search-space.md)
- [`ExploreSpaceSobolQMC`](explore-space.md)
- [`OptimizeSpaceBotorch`](optimize-space.md)
- [How to resume a search](../how-to-guides/resume-a-search.md)
