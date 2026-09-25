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
so both classes wait
with [`RaiseOnError.RAISE_AFTER_COMPLETED`](executor.md#raiseonerror).
They turn what came back into a `RuntimeError` that names the studies that failed,
rather than feed a `RemoteExecutionError` into a model.

One bad evaluation therefore does not hide the rest of its batch.
Both classes record what did come back before they raise the exception.
`save()` therefore still holds the good points,
and the next run resumes from them.

This holds for an objective that raises.
An objective that returns a result the contract rejects,
such as a bare float or a `NaN`,
raises as soon as its class records it,
and the points after it in submission order are not recorded.

## Related

- [Search spaces](search-space.md)
- [`ExploreSpaceSobolQMC`](explore-space.md)
- [`OptimizeSpaceBotorch`](optimize-space.md)
