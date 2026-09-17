"""Batch Bayesian optimization of search spaces, with botorch.

Fits a Gaussian process to everything measured so far,
and asks it for a whole batch of points at once.
Evaluates that batch across a pilot pool, refits, repeats.
This module does not explore.
A run starts from the results files `ExploreSpaceSobolQMC.save` wrote.

See `docs/reference/optimize-space.md` for what a round does and how a search stops.
"""

from __future__ import annotations

import gzip
import pickle
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import torch
from botorch.models import SingleTaskGP
from botorch.models.transforms import Standardize
from botorch.fit import fit_gpytorch_mll
from botorch.optim import optimize_acqf
from botorch.acquisition import qLogNoisyExpectedImprovement
from botorch.sampling import SobolQMCNormalSampler
from gpytorch.mlls import ExactMarginalLogLikelihood

from .slurm_pilot_executor import SlurmPilotExecutor, RaiseOnError, Task
from .search_space import SearchSpace, space_dim, to_params, to_unit
from .explore_space import SavedResults, load_results
from .utils import (
    RemoteExecutionError,
    format_mapping,
    index_width,
    objective_value,
)

# Botorch recommendation: single precision is numerically fragile here.
DTYPE = torch.double


ObjectiveOutput = Mapping[str, Any]
ObjectiveFunction = Callable[..., ObjectiveOutput]


# How far outside the unit cube a saved point can land
# before this module takes it for a point from another space.
SAVED_POINT_TOLERANCE = 1e-9

CANDIDATES_KEY = "candidates"
FIT_SECONDS_KEY = "fit_seconds"
PROPOSE_SECONDS_KEY = "propose_seconds"


@dataclass
class OptimizationStudy:
    """One space to optimize, and everything needed to optimize it.

    name: keys its results,
        and must match the name the results files hold its observations under.
    objective: the search minimizes it.
        Its argument names must match the keys of `space`,
        and it returns a mapping carrying `objective_key`.
    optimizer_queue: where the model fit and the acquisition optimization run,
        one task per round.
        Its workers need botorch.
        The objective's workers do not.
    search_parallelism: points evaluated per round.
        When None, the count comes from the search.

    The search runs between `min_search_rounds`
    and `max_search_rounds` rounds.
    It stops early when it stops improving:

    min_search_rounds: rounds that always run.
        Stalled rounds below it count toward patience.
        But they cannot end the search.
    max_search_rounds: hard ceiling.
    patience: consecutive stalled rounds that end the search.
        A round that improves resets the count.
    min_improvement: fraction of the incumbent's magnitude
        a round must beat it by.
        A smaller gain counts as a stall.

    objective_key: the key of the result to minimize.
        The search records every other key and does not model it.

    The rest tune the fit and the acquisition optimization:

    num_restarts: multi-start count for the acquisition optimization.
    raw_samples: candidates drawn to pick those starting points from.
    mc_samples: quasi-MC draws used to estimate the acquisition value
        at a candidate.
    acqf_timeout_s: wall-clock budget for one propose step.
        A timeout is not an error.
        The step returns the best candidates so far.

    extra_objective_kwargs: extra keyword arguments for the objective.
        Must not shadow a parameter of the space.
    """

    name: str
    space: SearchSpace
    objective: ObjectiveFunction
    objective_queue: str | list[str]
    optimizer_queue: str | list[str]
    search_parallelism: int | None = None
    min_search_rounds: int = 5
    max_search_rounds: int = 30
    patience: int = 3
    min_improvement: float = 0.05
    objective_key: str = "objective"
    num_restarts: int = 10
    raw_samples: int = 128
    mc_samples: int = 128
    acqf_timeout_s: float = 10.0
    extra_objective_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class OptimizationResult:
    """What one study measured, in submission order.

    The four lists are index-aligned.
    The search evaluates `points[i]`, gets `outputs[i]` back,
    models the point by `values[i]`,
    and records `unit_points[i]` as its place in the unit cube.
    `unit_points` is where the objective ran, after any rounding.
    """

    points: list[dict[str, Any]] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    # The objective's whole result, not just the number modeled from it.
    outputs: list[dict[str, Any]] = field(default_factory=list)
    unit_points: list[list[float]] = field(default_factory=list)


def fit_and_propose(
    unit_points: list[list[float]],
    values: list[float],
    batch: int,
    *,
    num_restarts: int,
    raw_samples: int,
    mc_samples: int,
    timeout_s: float,
) -> dict[str, Any]:
    """Fit the GP and optimize the acquisition over the unit cube.

    Returns the `batch` proposed unit points under `CANDIDATES_KEY`,
    and how long each half took under `FIT_SECONDS_KEY`
    and `PROPOSE_SECONDS_KEY`.
    """
    train_x = torch.tensor(unit_points, dtype=DTYPE)

    # Botorch maximizes and the search minimizes the objective.
    # So this function fits the model to -f,
    # and the acquisition values below are in that space.
    train_y = torch.tensor([[-v] for v in values], dtype=DTYPE)

    model = SingleTaskGP(train_x, train_y, outcome_transform=Standardize(m=1))
    mll = ExactMarginalLogLikelihood(model.likelihood, model)

    started = time.monotonic()
    fit_gpytorch_mll(mll)
    fit_seconds = time.monotonic() - started

    dim = train_x.shape[-1]
    bounds = torch.stack([torch.zeros(dim, dtype=DTYPE), torch.ones(dim, dtype=DTYPE)])

    acqf = qLogNoisyExpectedImprovement(
        model,
        train_x,
        sampler=SobolQMCNormalSampler(torch.Size([mc_samples])),
    )

    started = time.monotonic()

    # The whole batch in one call, optimized jointly rather than greedily.
    candidates, _ = optimize_acqf(
        acqf,
        bounds=bounds,
        q=batch,
        num_restarts=num_restarts,
        raw_samples=raw_samples,
        timeout_sec=timeout_s,
    )

    propose_seconds = time.monotonic() - started

    return {
        CANDIDATES_KEY: candidates.tolist(),
        FIT_SECONDS_KEY: fit_seconds,
        PROPOSE_SECONDS_KEY: propose_seconds,
    }


class OptimizeSpaceBotorch:
    """Botorch batch optimization of one or more search spaces, run together.

    The search rounds integer and categorical parameters
    from a continuous candidate.
    So a mostly-discrete space re-evaluates points.
    """

    def __init__(
        self,
        studies: list[OptimizationStudy],
        executor: SlurmPilotExecutor,
        files: Iterable[Path | str],
        search_parallelism: int | None = None,
    ) -> None:
        """Validate every study and load the observations it starts from.

        The studies all run together, in the same rounds.
        Each drops out when it meets its own stopping rule.
        files: results files to start from,
            as `ExploreSpaceSobolQMC.save` or this class's own `save` wrote them.
            The search models a study on every observation
            they hold under its name.
            A study with none of them raises.
        search_parallelism: batch size for studies that do not carry their own.
            A study with neither raises.

        The search validates every study now, not when it runs.
        """
        if not studies:
            raise ValueError("no optimization studies given")

        names = [study.name for study in studies]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"optimization study names must be unique: {duplicates}")

        self.executor = executor
        self.studies = [self._resolve(study, search_parallelism) for study in studies]

        # What the files hold, keyed by study name.
        # `save` does not write it back.
        self.prior: dict[str, OptimizationResult] = {}
        # What this instance evaluated, which is what `save` writes.
        self.results: dict[str, OptimizationResult] = {}

        loaded = load_results(files)
        for study in self.studies:
            self.prior[study.name] = self._observed(study, loaded.get(study.name))
            self.results[study.name] = OptimizationResult()

    @staticmethod
    def _resolve(
        study: OptimizationStudy, default_parallelism: int | None
    ) -> OptimizationStudy:
        """Validate one study and fill in what it left to the run."""
        if not study.space:
            raise ValueError(f"{study.name}: search space is empty")

        overlap = set(study.extra_objective_kwargs) & set(study.space)
        if overlap:
            raise ValueError(
                f"{study.name}: extra_objective_kwargs may not shadow search "
                f"space parameters: {sorted(overlap)}"
            )

        parallelism = study.search_parallelism
        if parallelism is None:
            parallelism = default_parallelism
        if parallelism is None:
            raise ValueError(
                f"{study.name}: no search_parallelism, on the study or on the run"
            )
        if parallelism < 1:
            raise ValueError(
                f"{study.name}: search_parallelism must be >= 1, got {parallelism}"
            )

        if study.min_search_rounds < 0:
            raise ValueError(
                f"{study.name}: min_search_rounds must be >= 0, "
                f"got {study.min_search_rounds}"
            )
        if study.max_search_rounds < study.min_search_rounds:
            raise ValueError(
                f"{study.name}: max_search_rounds must be >= "
                f"min_search_rounds, got {study.max_search_rounds} < "
                f"{study.min_search_rounds}"
            )
        if study.patience < 1:
            raise ValueError(
                f"{study.name}: patience must be >= 1, got {study.patience}"
            )
        if study.min_improvement < 0.0:
            raise ValueError(
                f"{study.name}: min_improvement must be >= 0, "
                f"got {study.min_improvement}"
            )
        if study.num_restarts < 1:
            raise ValueError(
                f"{study.name}: num_restarts must be >= 1, got {study.num_restarts}"
            )
        if study.raw_samples < 1:
            raise ValueError(
                f"{study.name}: raw_samples must be >= 1, got {study.raw_samples}"
            )
        if study.mc_samples < 1:
            raise ValueError(
                f"{study.name}: mc_samples must be >= 1, got {study.mc_samples}"
            )
        if study.acqf_timeout_s <= 0.0:
            raise ValueError(
                f"{study.name}: acqf_timeout_s must be > 0, got {study.acqf_timeout_s}"
            )

        return replace(study, space=dict(study.space), search_parallelism=parallelism)

    @staticmethod
    def _observed(
        study: OptimizationStudy, saved: SavedResults | None
    ) -> OptimizationResult:
        """A study's starting observations, standardized into its own space."""
        if saved is None or not saved.values:
            raise RuntimeError(
                f"{study.name}: no observations in the given files; "
                "explore the space first, and pass what "
                "ExploreSpaceSobolQMC.save() wrote"
            )

        unit_points = [
            OptimizeSpaceBotorch._saved_unit_point(study, params)
            for params in saved.points
        ]

        for value in saved.values:
            objective_value(study.name, "saved value", {}, {"saved value": value})

        return OptimizationResult(
            points=list(saved.points),
            values=[float(value) for value in saved.values],
            outputs=list(saved.outputs),
            unit_points=unit_points,
        )

    @staticmethod
    def _saved_unit_point(
        study: OptimizationStudy, params: Mapping[str, Any]
    ) -> list[float]:
        """One saved point, standardized into a study's own space.

        Raises if the space cannot place it.
        """
        mismatch = set(params) ^ set(study.space)
        if mismatch:
            raise RuntimeError(
                f"{study.name}: a saved point has parameters {sorted(params)}, "
                f"which do not match the search space {sorted(study.space)}"
            )

        try:
            unit = to_unit(study.space, params)
        except ValueError as e:
            # A log range cannot standardize a value at or below zero.
            raise RuntimeError(
                f"{study.name}: a saved point {dict(params)} cannot be placed "
                f"in the search space {sorted(study.space)}: {e}"
            ) from e

        outside = [
            name
            for name, coordinate in zip(study.space, unit)
            if not -SAVED_POINT_TOLERANCE <= coordinate <= 1.0 + SAVED_POINT_TOLERANCE
        ]
        if outside:
            raise RuntimeError(
                f"{study.name}: a saved point {dict(params)} lies outside the "
                f"search space in {sorted(outside)}; it was measured over a "
                "different range, and the model may not be fit on it"
            )

        return unit

    def _study(self, name: str) -> OptimizationStudy:
        """The named study, or a `KeyError` that lists the studies there are."""
        for study in self.studies:
            if study.name == name:
                return study
        raise KeyError(
            f"no optimization study named {name!r}; have {sorted(self.results)}"
        )

    def dim(self, name: str) -> int:
        """Dimensionality of a study's search space."""
        return space_dim(self._study(name).space)

    def observations(self, name: str) -> tuple[list[list[float]], list[float]]:
        """Everything a study's model uses: the files, then this run."""
        self._study(name)
        prior, results = self.prior[name], self.results[name]
        return (
            prior.unit_points + results.unit_points,
            prior.values + results.values,
        )

    def num_observations(self, name: str) -> int:
        """How many points a study's model uses."""
        return len(self.observations(name)[1])

    def run(self) -> None:
        """Run search rounds for every study until each one is done.

        A round is one fit per still-running study,
        then every study's proposed batch, evaluated on its objective queue.
        Tasks advance in step and drop out independently,
        each on its own patience and ceiling.
        A second call runs another set of rounds from where this stopped.

        The search names the two tasks of a round
        `<study>-fit-<round>` and `<study>-search-<round>-<index>`
        on the server.
        """
        active = list(self.studies)
        stalled = {study.name: 0 for study in self.studies}

        round_number = 0
        while active:
            round_number += 1
            desc = f"search round {round_number}"

            previous_best = {
                study.name: self._best_value(study.name) for study in active
            }

            proposals = self._fit_and_propose(active, desc, round_number)
            self._evaluate(
                {
                    study.name: [
                        to_params(study.space, candidate)
                        for candidate in proposals[study.name]
                    ]
                    for study in active
                },
                desc,
                round_number,
            )

            still_running = []
            for study in active:
                self._report_best(study.name)

                if self._improved_enough(
                    study, previous_best[study.name], self._best_value(study.name)
                ):
                    stalled[study.name] = 0
                    still_running.append(study)
                    continue

                stalled[study.name] += 1

                # Whichever bound is further away: the streak reaching
                # `patience`, or the rounds reaching the floor.
                remaining = max(
                    study.patience - stalled[study.name],
                    study.min_search_rounds - round_number,
                )

                if remaining > 0:
                    print(
                        f"{study.name}: round {round_number} improved by less "
                        f"than {study.min_improvement:.0%} "
                        f"--- {stalled[study.name]} in a row, "
                        f"{remaining} more to stop",
                        flush=True,
                    )
                    still_running.append(study)
                    continue

                print(
                    f"{study.name}: stopping after {round_number} rounds "
                    f"--- {stalled[study.name]} in a row without a "
                    f"{study.min_improvement:.0%} improvement",
                    flush=True,
                )

            active = [
                study
                for study in still_running
                if round_number < study.max_search_rounds
            ]
            for study in still_running:
                if study not in active:
                    print(
                        f"{study.name}: stopping after {round_number} rounds "
                        f"--- the ceiling on this search",
                        flush=True,
                    )

    def _fit_and_propose(
        self, studies: list[OptimizationStudy], desc: str, round_number: int
    ) -> dict[str, list[list[float]]]:
        """Submit one fit per study, then wait for all of them."""
        submissions: list[tuple[str, Task]] = []
        for study in studies:
            unit_points, values = self.observations(study.name)
            print(
                f"{study.name}: fitting GP on {len(values)} points ...",
                flush=True,
            )
            submission = self.executor.submit(
                study.optimizer_queue,
                fit_and_propose,
                unit_points,
                values,
                study.search_parallelism,
                num_restarts=study.num_restarts,
                raw_samples=study.raw_samples,
                mc_samples=study.mc_samples,
                timeout_s=study.acqf_timeout_s,
            )
            self.executor.set_task_name(submission, f"{study.name}-fit-{round_number}")
            submissions.append((study.name, submission))

        try:
            self.executor.wait(
                [submission for _, submission in submissions],
                desc=desc,
                unit="fit",
                raise_on_error=RaiseOnError.RAISE_AFTER_COMPLETED,
            )
        except RuntimeError as e:
            # Name every failure in full: the traceback is in a worker log,
            # and the usual cause is a worker that cannot import botorch.
            broken = [
                f"{name} on queue {self._study(name).optimizer_queue!r} "
                f"--- {submission.output}"
                for name, submission in submissions
                if isinstance(submission.output, RemoteExecutionError)
            ]
            detail = f": {'; '.join(broken)}" if broken else ""
            raise RuntimeError(f"fitting the model failed during {desc}{detail}") from e

        proposals = {}
        for study, (_, submission) in zip(studies, submissions):
            proposals[study.name] = self._candidates(study, submission, desc)
        return proposals

    def _candidates(
        self, study: OptimizationStudy, submission: Task, desc: str
    ) -> list[list[float]]:
        """The batch one fit proposed, checked before the run evaluates it."""
        # Check every key, not just the candidates,
        # so a stale worker fails with this message and not a KeyError.
        result = submission.output
        expected = (CANDIDATES_KEY, FIT_SECONDS_KEY, PROPOSE_SECONDS_KEY)
        if isinstance(result, Mapping):
            missing = [key for key in expected if key not in result]
        else:
            missing = list(expected)
        if missing:
            raise RuntimeError(
                f"{study.name}: the optimizer queue returned {result!r} "
                f"during {desc}, with no "
                f"{', '.join(repr(key) for key in missing)} in it; "
                "check that its workers run the same slurm-workflows "
                "as this driver"
            )

        candidates = result[CANDIDATES_KEY]

        # Every round is the full width of the pool.
        # A short batch narrows it silently.
        if len(candidates) != study.search_parallelism:
            raise RuntimeError(
                f"{study.name}: the optimizer queue proposed "
                f"{len(candidates)} points during {desc}, not the "
                f"{study.search_parallelism} asked for"
            )

        print(
            f"{study.name}: GP fit took {result[FIT_SECONDS_KEY]:.2f}s, "
            f"proposed {len(candidates)} points in "
            f"{result[PROPOSE_SECONDS_KEY]:.2f}s",
            flush=True,
        )

        return candidates

    def _evaluate(
        self,
        batches: dict[str, list[dict[str, Any]]],
        desc: str,
        round_number: int,
    ) -> None:
        """Evaluate every study's batch together and record the results."""
        submitted: list[tuple[OptimizationStudy, dict[str, Any], Task]] = []
        for name, points in batches.items():
            study = self._study(name)
            width = index_width(len(points))
            for i, params in enumerate(points):
                submission = self.executor.submit(
                    study.objective_queue,
                    study.objective,
                    **params,
                    **study.extra_objective_kwargs,
                )
                self.executor.set_task_name(
                    submission, f"{study.name}-search-{round_number}-{i:0{width}d}"
                )
                submitted.append((study, params, submission))

        try:
            self._wait(
                [(study.name, submission) for study, _, submission in submitted],
                desc,
                unit="point",
                what="objective evaluations",
            )
        except RuntimeError:
            # Keep what did come back before reporting the failure.
            self._record_returned(submitted)
            raise

        for study, params, submission in submitted:
            self._record(study, params, submission)

    def _wait(
        self, submissions: list[tuple[str, Task]], desc: str, unit: str, what: str
    ) -> None:
        """Wait for a whole batch, and name the studies that failed."""
        try:
            self.executor.wait(
                [submission for _, submission in submissions],
                desc=desc,
                unit=unit,
                raise_on_error=RaiseOnError.RAISE_AFTER_COMPLETED,
            )
        except RuntimeError as e:
            failed = sorted(
                {
                    name
                    for name, submission in submissions
                    if isinstance(submission.output, RemoteExecutionError)
                }
            )
            # Empty when nothing came back at all, for example after a canceled task.
            # Then the cause is in the exception this chains to.
            named = f" of {failed}" if failed else ""
            raise RuntimeError(f"{what} failed during {desc}{named}") from e

    def _record_returned(
        self, submitted: list[tuple[OptimizationStudy, dict[str, Any], Task]]
    ) -> None:
        """Record every evaluation that came back. For the failure path only."""
        for study, params, submission in submitted:
            try:
                self._record(study, params, submission)
            except RuntimeError:
                continue

    def _record(
        self, study: OptimizationStudy, params: dict[str, Any], submission: Task
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

    def _improved_enough(
        self, study: OptimizationStudy, previous: float, current: float
    ) -> bool:
        """Whether `current` beats `previous` by at least `min_improvement`.

        The threshold is a fraction of the incumbent's magnitude.
        Against an incumbent of exactly zero, any strict decrease counts.
        """
        if current >= previous:
            return False

        magnitude = abs(previous)
        if magnitude == 0.0:
            return True

        return (previous - current) / magnitude >= study.min_improvement

    def _all(self, name: str) -> OptimizationResult:
        """One study's observations, the files and this run together."""
        prior, results = self.prior[name], self.results[name]
        return OptimizationResult(
            points=prior.points + results.points,
            values=prior.values + results.values,
            outputs=prior.outputs + results.outputs,
            unit_points=prior.unit_points + results.unit_points,
        )

    def _best_value(self, name: str) -> float:
        """The lowest value a study knows of, from the files or this run."""
        return min(self.prior[name].values + self.results[name].values)

    def _report_best(self, name: str) -> None:
        """Print the best point a study knows of."""
        known = self._all(name)
        best = min(range(len(known.values)), key=known.values.__getitem__)
        params = format_mapping(known.points[best])
        # The whole result, not just the objective value.
        output = format_mapping(known.outputs[best])
        print(
            f"{name}: best after {len(known.values)} points "
            f"at {params} -> {output}",
            flush=True,
        )

    def best_point(self, name: str) -> tuple[dict[str, Any], float]:
        """A study's best point (params, objective value) known so far.

        Over the files it started from as well as this run.
        """
        known = self._all(self._study(name).name)
        best = min(range(len(known.values)), key=known.values.__getitem__)
        return dict(known.points[best]), known.values[best]

    def best_output(self, name: str) -> dict[str, Any]:
        """The objective's whole result at a study's best point so far."""
        known = self._all(self._study(name).name)
        best = min(range(len(known.values)), key=known.values.__getitem__)
        return dict(known.outputs[best])

    def save(self, path: Path | str) -> None:
        """Write what this run measured to a gzipped pickle.

        The file holds only this run.
        So the caller can pass the files it started from and this one
        to the next `OptimizeSpaceBotorch` together,
        without counting a point twice.
        Same shape as `ExploreSpaceSobolQMC.save` writes.

        Overwrites `path`.
        If the search evaluated nothing, it writes empty lists.
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
