"""HPC workflow helpers for Slurm clusters.

Everything a program is written against is importable from here.
`OptimizeSpaceBotorch` and `OptimizationTask` resolve on first use,
so `import slurm_workflows` works without botorch installed.
"""

from typing import TYPE_CHECKING, Any

from .slurm_pilot_executor import SlurmPilotExecutor, RaiseOnError, Task
from .search_space import IntRange, FloatRange, CategoricalRange
from .explore_space import (
    ExplorationTask,
    ExploreSpaceSobolQMC,
    SavedResults,
    load_results,
)
from .utils import RemoteExecutionError

if TYPE_CHECKING:
    from .optimize_space_botorch import OptimizationTask, OptimizeSpaceBotorch

_BOTORCH_NAMES = ("OptimizationTask", "OptimizeSpaceBotorch")

__all__ = [
    "SlurmPilotExecutor",
    "RaiseOnError",
    "Task",
    "RemoteExecutionError",
    "IntRange",
    "FloatRange",
    "CategoricalRange",
    "ExplorationTask",
    "ExploreSpaceSobolQMC",
    "SavedResults",
    "load_results",
    "OptimizationTask",
    "OptimizeSpaceBotorch",
]


def __getattr__(name: str) -> Any:
    """Resolve the botorch names on first use."""
    if name in _BOTORCH_NAMES:
        from . import optimize_space_botorch

        return getattr(optimize_space_botorch, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
