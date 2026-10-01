"""HPC workflow helpers for Slurm clusters."""

from typing import Any

from .slurm_pilot_executor import SlurmPilotExecutor, RaiseOnError, Task
from .utils import RemoteExecutionError

# `NoOutput` is not exported here.
# It lives in `slurm_pilot_executor`.
__all__ = [
    "SlurmPilotExecutor",
    "RaiseOnError",
    "Task",
    "RemoteExecutionError",
]

# The names that moved to `slurm-workflows-optimize` in 5.0.
# The 5.x series still names the new home of each one.
_MOVED_NAMES = (
    "IntRange",
    "FloatRange",
    "CategoricalRange",
    "ExplorationStudy",
    "ExploreSpaceSobolQMC",
    "SavedResults",
    "load_results",
    "OptimizationStudy",
    "OptimizeSpaceBotorch",
)


def __getattr__(name: str) -> Any:
    """Name the new home of a name that moved out of this package."""
    if name in _MOVED_NAMES:
        raise ImportError(
            f"{name} moved to the slurm-workflows-optimize package in "
            f"slurm-workflows 5.0. Install it with "
            f"`pip install slurm-workflows-optimize`, "
            f"then import it with `from slurm_workflows_optimize import {name}`."
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
