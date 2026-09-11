"""Search spaces: what an optimizer can vary, and over what.

A search space maps parameter names to ranges.
Every range moves one of its own values into `[0, 1]` and back again.
So an optimizer works in the unit cube.
The objective sees values of the kind it declared.
Nothing here imports torch or botorch.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass
class IntRange:
    """Integer range, inclusive of both bounds.

    `max` must be greater than `min`.
    """

    min: int
    max: int

    def __post_init__(self) -> None:
        if self.max <= self.min:
            raise ValueError(f"IntRange needs max > min, got {self.min}, {self.max}")

    def standardize(self, x: int) -> float:
        """Move from [min, max] range to [0, 1] range."""
        return (x - self.min) / (self.max - self.min)

    def unstandardize(self, y: float) -> int:
        """Move from [0, 1] range to the nearest integer in [min, max]."""
        x = round(self.min + y * (self.max - self.min))
        return int(min(max(x, self.min), self.max))


@dataclass
class FloatRange:
    """Floating-point range, inclusive of both bounds.

    `max` must be greater than `min`.
    With `log_range`, an optimizer searches the range in log space,
    so every decade gets an equal share of the budget.
    `log_range` also needs `min` above zero.
    """

    min: float
    max: float
    log_range: bool = False

    def __post_init__(self) -> None:
        if self.max <= self.min:
            raise ValueError(f"FloatRange needs max > min, got {self.min}, {self.max}")
        if self.log_range and self.min <= 0.0:
            raise ValueError(f"log_range needs min > 0, got {self.min}")

    def standardize(self, x: float) -> float:
        """Move from [min, max] range to [0, 1] range."""
        if self.log_range:
            # Both ends and the value into log space, so the mapping back
            # in `unstandardize` is the exact inverse.
            lo, hi, x = math.log(self.min), math.log(self.max), math.log(x)
        else:
            lo, hi = self.min, self.max
        return (x - lo) / (hi - lo)

    def unstandardize(self, y: float) -> float:
        """Move from [0, 1] range to [min, max] range."""
        y = min(max(y, 0.0), 1.0)
        if self.log_range:
            lo, hi = math.log(self.min), math.log(self.max)
            return math.exp(lo + y * (hi - lo))
        return self.min + y * (self.max - self.min)


@dataclass
class CategoricalRange:
    """Categorical range, standardized as an index in `[0, n - 1]`.

    `num_categories` must be at least 1.
    `CategoricalRange` accepts `num_categories=1`,
    unlike a degenerate `IntRange`.
    But one category is a dead dimension.
    Prefer `extra_objective_kwargs` for that parameter.
    """

    num_categories: int

    def __post_init__(self) -> None:
        if self.num_categories < 1:
            raise ValueError(f"num_categories must be >= 1, got {self.num_categories}")

    def standardize(self, x: int) -> float:
        """Move from [0, num_categories - 1] range to [0, 1] range."""
        if self.num_categories == 1:
            return 0.0
        return x / (self.num_categories - 1)

    def unstandardize(self, y: float) -> int:
        """Move from [0, 1] range to [0, num_categories - 1] range."""
        if self.num_categories == 1:
            return 0
        i = round(y * (self.num_categories - 1))
        return int(min(max(i, 0), self.num_categories - 1))


ParameterRange = IntRange | FloatRange | CategoricalRange

SearchSpace = Mapping[str, ParameterRange]


def space_dim(space: SearchSpace) -> int:
    """Dimensionality of a search space."""
    return len(space)


def to_params(space: SearchSpace, unit: Sequence[float]) -> dict[str, Any]:
    """Unit cube coordinates -> objective keyword arguments.

    The coordinates follow the order of the space.
    `to_unit` produces them in that same order.
    """
    return {
        name: range_.unstandardize(float(u))
        for (name, range_), u in zip(space.items(), unit)
    }


def to_unit(space: SearchSpace, params: Mapping[str, Any]) -> list[float]:
    """Objective keyword arguments -> unit cube coordinates."""
    return [range_.standardize(params[name]) for name, range_ in space.items()]
