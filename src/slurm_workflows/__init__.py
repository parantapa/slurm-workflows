"""HPC workflow helpers for Slurm clusters."""

from typing import TYPE_CHECKING, Any

from .slurm_pilot_executor import SlurmPilotExecutor, RaiseOnError, Task
from .search_space import IntRange, FloatRange, CategoricalRange
from .explore_space import (
    ExplorationStudy,
    ExploreSpaceSobolQMC,
    SavedResults,
    load_results,
)
from .utils import RemoteExecutionError

# Never imported at runtime, since botorch is optional.
# See the developer notes, Batch Bayesian optimization.
if TYPE_CHECKING:
    from .optimize_space_botorch import OptimizationStudy, OptimizeSpaceBotorch

_BOTORCH_NAMES = ("OptimizationStudy", "OptimizeSpaceBotorch")

# `NoOutput` is not exported here.
# It lives in `slurm_pilot_executor`.
__all__ = [
    "SlurmPilotExecutor",
    "RaiseOnError",
    "Task",
    "RemoteExecutionError",
    "IntRange",
    "FloatRange",
    "CategoricalRange",
    "ExplorationStudy",
    "ExploreSpaceSobolQMC",
    "SavedResults",
    "load_results",
    "OptimizationStudy",
    "OptimizeSpaceBotorch",
]


def __getattr__(name: str) -> Any:
    """Resolve the botorch names on first use."""
    if name in _BOTORCH_NAMES:
        from . import optimize_space_botorch

        return getattr(optimize_space_botorch, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
