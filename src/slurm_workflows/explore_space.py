"""Sobol' QMC exploration of search spaces.

Draws a low-discrepancy design over each `SearchSpace`,
evaluates every point of every design across a pilot pool,
and keeps what came back.
Needs neither torch nor botorch.

See `docs/reference/explore-space.md` for what a sweep is for and how it behaves.
"""

from __future__ import annotations

import gzip
import os
import pickle
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace

from scipy.stats import qmc

from .slurm_pilot_executor import SlurmPilotExecutor, RaiseOnError, Task
from .search_space import SearchSpace, space_dim, to_params, to_unit
from .utils import (
    RemoteExecutionError,
    floor_power_of_two,
    format_mapping,
    index_width,
    objective_value,
)

ObjectiveOutput = Mapping[str, Any]
ObjectiveFunction = Callable[..., ObjectiveOutput]


@dataclass
class ExplorationTask:
    """One space to explore, and everything needed to explore it.

    name: keys its results, and must be unique within a sweep.
    objective: its argument names must match the keys of `space`,
        and it returns a mapping carrying `objective_key`.
    num_exploration_points: number of points to sample.
        The sweep truncates it to the nearest lower power of two.
        When None, the count comes from the sweep.
    seed: seed for this task's design.
        When None, the sweep draws one from `os.urandom` and prints it.
    objective_key: the key of the result to rank points by, lower first.
        The sweep records every other key and does not rank it.
    extra_objective_kwargs: extra keyword arguments for the objective.
        Must not shadow a parameter of the space.
    """

    name: str
    space: SearchSpace
    objective: ObjectiveFunction
    objective_queue: str | list[str]
    num_exploration_points: int | None = None
    seed: int | None = None
    objective_key: str = "objective"
    extra_objective_kwargs: dict = field(default_factory=dict)


@dataclass
class SavedResults:
    """Exactly what a results file holds for one task.

    `unit_points` is not in the file.
    A reader recomputes it.
    """

    points: list[dict[str, Any]] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)


def load_results(paths: Iterable[Path | str]) -> dict[str, SavedResults]:
    """Read back results files, merged by task name in the order given.

    Reads what `ExploreSpaceSobolQMC.save` and `OptimizeSpaceBotorch.save` write.
    That file is a gzipped pickle of one dict keyed by task name.
    Each entry holds `points`, `values` and `outputs`.
    The lists of one task join end to end, in the order of the paths.
    """
    merged: dict[str, SavedResults] = {}

    for path in paths:
        with gzip.open(path, "rb") as fobj:
            loaded = pickle.load(fobj)

        if not isinstance(loaded, Mapping):
            raise ValueError(f"{path}: expected a mapping of task name to results")

        for name, results in loaded.items():
            if not isinstance(results, Mapping) or not {
                "points",
                "values",
                "outputs",
            } <= set(results):
                raise ValueError(
                    f"{path}: {name!r} does not hold points, values and outputs"
                )

            lengths = {len(results[key]) for key in ("points", "values", "outputs")}
            if len(lengths) != 1:
                raise ValueError(
                    f"{path}: {name!r} holds lists of different lengths, "
                    "so its points, values and outputs do not line up"
                )

            saved = merged.setdefault(name, SavedResults())
            saved.points.extend(results["points"])
            saved.values.extend(results["values"])
            saved.outputs.extend(results["outputs"])

    return merged


@dataclass
class ExplorationResult:
    """What one task measured, in submission order.

    The four lists are index-aligned.
    The sweep evaluates `points[i]`, gets `outputs[i]` back,
    ranks the point by `values[i]`,
    and records `unit_points[i]` as its place in the unit cube.
    `unit_points` is where the objective ran, after any rounding.
    """

    points: list[dict[str, Any]] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    # The objective's whole result, not just the number ranked from it.
    outputs: list[dict[str, Any]] = field(default_factory=list)
    unit_points: list[list[float]] = field(default_factory=list)


class ExploreSpaceSobolQMC:
    """Sobol' QMC sweeps of one or more search spaces, run together."""

    def __init__(
        self,
        tasks: list[ExplorationTask],
        executor: SlurmPilotExecutor,
        num_exploration_points: int | None = None,
    ) -> None:
        """Validate every task and fill in what it left to the sweep.

        The tasks all run together,
        so a small sweep does not wait on a large one.
        `num_exploration_points` is the count for tasks that do not carry their own.
        A task with neither raises.
        The sweep validates every task now, not when it runs.
        `self.tasks` holds copies with the point count and seed filled in.
        The caller's own objects stay as they are.
        """
        if not tasks:
            raise ValueError("no exploration tasks given")

        names = [task.name for task in tasks]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"exploration task names must be unique: {duplicates}")

        self.executor = executor
        self.tasks = [self._resolve(task, num_exploration_points) for task in tasks]
        self.results: dict[str, ExplorationResult] = {
            task.name: ExplorationResult() for task in self.tasks
        }

    @staticmethod
    def _resolve(task: ExplorationTask, default_points: int | None) -> ExplorationTask:
        """Validate one task and fill in what it left to the sweep."""
        if not task.space:
            raise ValueError(f"{task.name}: search space is empty")

        overlap = set(task.extra_objective_kwargs) & set(task.space)
        if overlap:
            raise ValueError(
                f"{task.name}: extra_objective_kwargs may not shadow search "
                f"space parameters: {sorted(overlap)}"
            )

        points = task.num_exploration_points
        if points is None:
            points = default_points
        if points is None:
            raise ValueError(
                f"{task.name}: no num_exploration_points, on the task or on "
                "the sweep"
            )

        seed = task.seed
        if seed is None:
            seed = int.from_bytes(os.urandom(8), "big")
            print(
                f"{task.name}: no seed given, drew {seed} "
                f"--- pass it back to repeat this run",
                flush=True,
            )

        # Sobol' is only balanced on power-of-two prefixes of the sequence.
        # So this method truncates the count to keep the design low-discrepancy.
        return replace(
            task,
            space=dict(task.space),
            num_exploration_points=floor_power_of_two(points),
            seed=seed,
        )

    def _task(self, name: str) -> ExplorationTask:
        """The named task, or a `KeyError` that lists the tasks there are."""
        for task in self.tasks:
            if task.name == name:
                return task
        raise KeyError(
            f"no exploration task named {name!r}; have {sorted(self.results)}"
        )

    def dim(self, name: str) -> int:
        """Dimensionality of a task's search space."""
        return space_dim(self._task(name).space)

    def design(self, name: str) -> list[dict[str, Any]]:
        """The points a task will evaluate, without evaluating them.

        Reproducible: the same seed redraws the same design.
        """
        task = self._task(name)
        assert task.num_exploration_points is not None  # _resolve fills it in
        assert task.seed is not None

        # `random_base2`: `_resolve` already floored the count to a power of two,
        # which is the form scipy takes without warning.
        engine = qmc.Sobol(d=space_dim(task.space), scramble=True, rng=task.seed)
        design = engine.random_base2(m=task.num_exploration_points.bit_length() - 1)
        return [to_params(task.space, row.tolist()) for row in design]

    def run(self) -> None:
        """Evaluate every task's design, all of them in one batch.

        Blocks until every point of every task is back.
        A second call re-evaluates the same designs.
        The sweep names each point `<task>-explore-<index>` on the queue server.
        """
        submitted: list[tuple[ExplorationTask, dict[str, Any], Task]] = []
        for task in self.tasks:
            design = self.design(task.name)
            width = index_width(len(design))
            for i, params in enumerate(design):
                submission = self.executor.submit(
                    task.objective_queue,
                    task.objective,
                    **params,
                    **task.extra_objective_kwargs,
                )
                self.executor.set_task_name(
                    submission, f"{task.name}-explore-{i:0{width}d}"
                )
                submitted.append((task, params, submission))

        try:
            self._wait(submitted)
        except RuntimeError:
            # Keep what did come back before reporting the failure.
            self._record_returned(submitted)
            raise

        for task, params, submission in submitted:
            self._record(task, params, submission)

        for task in self.tasks:
            self._report_best(task.name)

    def _wait(
        self, submitted: list[tuple[ExplorationTask, dict[str, Any], Task]]
    ) -> None:
        """Wait for the whole batch, and name the tasks that failed."""
        try:
            self.executor.wait(
                [submission for _, _, submission in submitted],
                desc="explore",
                unit="point",
                raise_on_error=RaiseOnError.RAISE_AFTER_COMPLETED,
            )
        except RuntimeError as e:
            failed = sorted(
                {
                    task.name
                    for task, _, submission in submitted
                    if isinstance(submission.output, RemoteExecutionError)
                }
            )
            # Empty when nothing came back at all, for example after a canceled task.
            # Then the cause is in the exception this chains to.
            named = f" of {failed}" if failed else ""
            raise RuntimeError(
                f"objective evaluations failed during exploration{named}"
            ) from e

    def _record_returned(
        self, submitted: list[tuple[ExplorationTask, dict[str, Any], Task]]
    ) -> None:
        """Record every evaluation that came back. For the failure path only."""
        for task, params, submission in submitted:
            try:
                self._record(task, params, submission)
            except RuntimeError:
                continue

    def _record(
        self, task: ExplorationTask, params: dict[str, Any], submission: Task
    ) -> None:
        """Check one evaluation's result and add it to its task's record."""
        output = submission.output
        value = objective_value(task.name, task.objective_key, params, output)

        result = self.results[task.name]
        result.points.append(params)
        result.values.append(value)
        # A copy, so a later change to the returned mapping
        # cannot rewrite what the run recorded.
        result.outputs.append(dict(output))
        result.unit_points.append(to_unit(task.space, params))

    def _report_best(self, name: str) -> None:
        """Print the best point one task measured."""
        best = self._best_index(name)
        result = self.results[name]
        params = format_mapping(result.points[best])
        # The whole result, not just the ranked value.
        output = format_mapping(result.outputs[best])
        print(
            f"{name}: best of {len(result.values)} points " f"at {params} -> {output}",
            flush=True,
        )

    def _best_index(self, name: str) -> int:
        """Index of the lowest objective value one task saw."""
        values = self.results[self._task(name).name].values
        if not values:
            raise RuntimeError(f"{name}: nothing has been evaluated yet")

        return min(range(len(values)), key=values.__getitem__)

    def best_point(self, name: str) -> tuple[dict[str, Any], float]:
        """A task's best point (params, objective value) so far."""
        best = self._best_index(name)
        result = self.results[name]
        return dict(result.points[best]), result.values[best]

    def best_output(self, name: str) -> dict[str, Any]:
        """The objective's whole result at a task's best point so far."""
        return dict(self.results[name].outputs[self._best_index(name)])

    def save(self, path: Path | str) -> None:
        """Write what every task measured to a gzipped pickle.

        The file holds one dict keyed by task name.
        Each entry holds `points`, `values` and `outputs`,
        index-aligned and in submission order.
        Read it back with `load_results`.
        Plain `pickle`, so an objective's result must be plainly picklable.
        Overwrites `path`.
        If the sweep evaluated nothing, it writes empty lists.
        """
        results = {
            name: {
                "points": result.points,
                "values": result.values,
                "outputs": result.outputs,
            }
            for name, result in self.results.items()
        }
        with gzip.open(path, "wb") as fobj:
            pickle.dump(results, fobj, protocol=pickle.HIGHEST_PROTOCOL)
