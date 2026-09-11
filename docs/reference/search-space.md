# Search spaces

[<- back to the main README](../../README.md)

`slurm_workflows.search_space`:
the parameter range types both space classes take.

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
| `CategoricalRange(n)` | An `int` in `[0, n - 1]` | An index into your own list of values. `n = 1` is allowed but is a dead dimension. |

One space can mix all three.
A parameter the search must not vary
belongs in `extra_objective_kwargs` rather than in the space.

Both space classes map every parameter into `[0, 1]` before a model sees it,
and map it back for the objective.
That is what lets one model span all the kinds at once.
The space rounds a continuous proposal
to get back an integer or a categorical parameter.
Both classes record where the objective actually ran, after rounding,
not the continuous proposal.

What that rounding costs a search on a mostly-discrete space is in
[About batch Bayesian optimization](../explanation/about-batch-bayesian-optimization.md).
