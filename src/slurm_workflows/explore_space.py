"""Sobol' QMC exploration of search spaces.

See `docs/reference/explore-space.md`
for what an exploration is for and how it behaves.
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
class ExplorationStudy:
    """One space to explore, and everything needed to explore it.

    name: keys its results, and must be unique within an exploration.
    objective: its argument names must match the keys of `space`,
        and it returns a mapping carrying `objective_key`.
    objective_queue: the queue, or queues, every evaluation goes to.
    num_exploration_points: number of points to sample.
        The exploration truncates it to the nearest lower power of two.
        When None, the count comes from the exploration.
    seed: seed for this study's design.
        When None, the exploration draws a random one and prints it.
    objective_key: the key of the result to rank points by, lower first.
        The exploration records every other key and does not rank it.
    extra_objective_kwargs: extra keyword arguments for the objective.
        Must not shadow a parameter of the space.
    priority: the priority of every task this study submits.
        The server dispatches the highest priority first.
    """

    name: str
    space: SearchSpace
    objective: ObjectiveFunction
    objective_queue: str | list[str]
    num_exploration_points: int | None = None
    seed: int | None = None
    objective_key: str = "objective"
    extra_objective_kwargs: dict[str, Any] = field(default_factory=dict)
    priority: float = 0.0


@dataclass
class SavedResults:
    """Exactly what a results file holds for one study.

    `unit_points` is not in the file.
    A reader recomputes it.
    """

    points: list[dict[str, Any]] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)


def load_results(paths: Iterable[Path | str]) -> dict[str, SavedResults]:
    """Read back results files, merged by study name in the order given.

    Reads what `ExploreSpaceSobolQMC.save` and `OptimizeSpaceBotorch.save` write.
    The lists of one study join end to end, in the order of the paths.
    Raises `ValueError` if a file does not hold the results shape,
    or if one study's lists differ in length.
    """
    merged: dict[str, SavedResults] = {}

    for path in paths:
        with gzip.open(path, "rb") as fobj:
            loaded = pickle.load(fobj)

        if not isinstance(loaded, Mapping):
            raise ValueError(f"{path}: expected a mapping of study name to results")

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
    """What one study measured, in submission order.

    The four lists are index-aligned.
    The exploration evaluates `points[i]`,
    gets the objective's whole result back as `outputs[i]`,
    ranks the point by `values[i]`,
    and records `unit_points[i]` as its place in the unit cube.
    `unit_points` is where the objective ran, after any rounding.
    """

    points: list[dict[str, Any]] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    unit_points: list[list[float]] = field(default_factory=list)


class ExploreSpaceSobolQMC:
    """Sobol' QMC explorations of one or more search spaces, run together.

    A method that takes a study name raises `KeyError` for a name no study has.
    """

    def __init__(
        self,
        studies: list[ExplorationStudy],
        executor: SlurmPilotExecutor,
        num_exploration_points: int | None = None,
    ) -> None:
        """Validate every study and fill in what it left to the exploration.

        `num_exploration_points` is the count for studies that do not carry their own.
        The exploration validates every study now, not when it runs.
        It raises `ValueError` for an empty list, for a repeated study name,
        and for a study that fails validation,
        such as one with no point count from either source.
        `self.studies` holds copies with the point count and seed filled in.
        The caller's own objects stay as they are.
        """
        if not studies:
            raise ValueError("no exploration studies given")

        names = [study.name for study in studies]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"exploration study names must be unique: {duplicates}")

        self.executor = executor
        # Validated here, so a bad study fails before anything reaches the cluster.
        # `_resolve` returns a copy, so the caller's dataclass stays as written.
        self.studies = [
            self._resolve(study, num_exploration_points) for study in studies
        ]
        self.results: dict[str, ExplorationResult] = {
            study.name: ExplorationResult() for study in self.studies
        }

    @staticmethod
    def _resolve(
        study: ExplorationStudy, default_points: int | None
    ) -> ExplorationStudy:
        """Validate one study and fill in what it left to the exploration."""
        if not study.space:
            raise ValueError(f"{study.name}: search space is empty")

        overlap = set(study.extra_objective_kwargs) & set(study.space)
        if overlap:
            raise ValueError(
                f"{study.name}: extra_objective_kwargs may not shadow search "
                f"space parameters: {sorted(overlap)}"
            )

        points = study.num_exploration_points
        if points is None:
            points = default_points
        if points is None:
            raise ValueError(
                f"{study.name}: no num_exploration_points, on the study or on "
                "the exploration"
            )

        seed = study.seed
        if seed is None:
            seed = int.from_bytes(os.urandom(8), "big")
            print(
                f"{study.name}: no seed given, drew {seed} "
                f"--- pass it back to repeat this run",
                flush=True,
            )

        # A power-of-two count:
        # a Sobol' sequence is only balanced on a power-of-two prefix.
        return replace(
            study,
            space=dict(study.space),
            num_exploration_points=floor_power_of_two(points),
            seed=seed,
        )

    def _study(self, name: str) -> ExplorationStudy:
        """The named study, or a `KeyError` that lists the studies there are."""
        for study in self.studies:
            if study.name == name:
                return study
        raise KeyError(
            f"no exploration study named {name!r}; have {sorted(self.results)}"
        )

    def dim(self, name: str) -> int:
        """Dimensionality of a study's search space."""
        return space_dim(self._study(name).space)

    def design(self, name: str) -> list[dict[str, Any]]:
        """The points a study will evaluate, without evaluating them.

        Reproducible: the same seed redraws the same design.
        """
        study = self._study(name)
        # `_resolve` fills in both.
        assert study.num_exploration_points is not None
        assert study.seed is not None

        # `random_base2`: `_resolve` already floored the count to a power of two,
        # which is the form scipy takes without warning.
        engine = qmc.Sobol(d=space_dim(study.space), scramble=True, rng=study.seed)
        design = engine.random_base2(m=study.num_exploration_points.bit_length() - 1)
        return [to_params(study.space, row.tolist()) for row in design]

    def run(self) -> None:
        """Evaluate every study's design, all of them in one batch.

        Blocks until every point of every study is back.
        A second call re-evaluates the same designs,
        and appends to the results the first call recorded.
        The exploration names each point `<study>-explore-<index>` on the server.
        It prints each study's best point when done.
        If any evaluation fails,
        it records every result that came back, then raises `RuntimeError`.
        """
        # Every submit comes before the one wait.
        # See the developer notes, Sobol' exploration.
        submitted: list[tuple[ExplorationStudy, dict[str, Any], Task]] = []
        for study in self.studies:
            design = self.design(study.name)
            width = index_width(len(design))
            for i, params in enumerate(design):
                submission = self.executor.submit(
                    study.objective_queue,
                    study.objective,
                    task_priority=study.priority,
                    **params,
                    **study.extra_objective_kwargs,
                )
                self.executor.set_task_name(
                    submission, f"{study.name}-explore-{i:0{width}d}"
                )
                submitted.append((study, params, submission))

        try:
            self._wait(submitted)
        except RuntimeError:
            # Keep what came back before re-raising.
            # See the developer notes, Task flow.
            self._record_returned(submitted)
            raise

        for study, params, submission in submitted:
            self._record(study, params, submission)

        for study in self.studies:
            self._report_best(study.name)

    def _wait(
        self, submitted: list[tuple[ExplorationStudy, dict[str, Any], Task]]
    ) -> None:
        """Wait for the whole batch, and name the studies that failed."""
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
                    study.name
                    for study, _, submission in submitted
                    if isinstance(submission.output, RemoteExecutionError)
                }
            )
            # Empty when no failure came back as an output,
            # for example when every failure is a canceled task.
            # Then the cause is in the exception this chains to.
            named = f" of {failed}" if failed else ""
            raise RuntimeError(
                f"objective evaluations failed during exploration{named}"
            ) from e

    def _record_returned(
        self, submitted: list[tuple[ExplorationStudy, dict[str, Any], Task]]
    ) -> None:
        """Record every evaluation that came back, on the failure path of `run`."""
        for study, params, submission in submitted:
            try:
                self._record(study, params, submission)
            except RuntimeError:
                # A failed evaluation has nothing to record,
                # and `run` re-raises the batch failure after this.
                continue

    def _record(
        self, study: ExplorationStudy, params: dict[str, Any], submission: Task
    ) -> None:
        """Check one evaluation's result and add it to its study's record."""
        output = submission.output
        value = objective_value(study.name, study.objective_key, params, output)

        result = self.results[study.name]
        result.points.append(params)
        result.values.append(value)
        # A copy, so a later change to the returned mapping
        # cannot rewrite what the run recorded.
        result.outputs.append(dict(output))
        result.unit_points.append(to_unit(study.space, params))

    def _report_best(self, name: str) -> None:
        """Print the best point one study measured."""
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
        """Index of the lowest objective value one study saw."""
        # Through `_study`, so an unknown name raises its `KeyError`.
        values = self.results[self._study(name).name].values
        if not values:
            raise RuntimeError(f"{name}: nothing has been evaluated yet")

        return min(range(len(values)), key=values.__getitem__)

    def best_point(self, name: str) -> tuple[dict[str, Any], float]:
        """A study's best point (params, objective value) so far.

        Raises `RuntimeError` if `run` has recorded nothing for the study yet.
        """
        best = self._best_index(name)
        result = self.results[name]
        return dict(result.points[best]), result.values[best]

    def best_output(self, name: str) -> dict[str, Any]:
        """The objective's whole result at a study's best point so far.

        Raises `RuntimeError` if `run` has recorded nothing for the study yet.
        """
        return dict(self.results[name].outputs[self._best_index(name)])

    def save(self, path: Path | str) -> None:
        """Write what every study measured to a gzipped pickle.

        The file holds one dict keyed by study name.
        Each entry holds `points`, `values` and `outputs`,
        index-aligned and in submission order.
        `load_results` reads it back.
        Plain `pickle`, so an objective's result must be plainly picklable.
        Overwrites `path`.
        If the exploration evaluated nothing, it writes empty lists.
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
