"""Tests for the Sobol' QMC exploration."""

from __future__ import annotations

# No importorskip for botorch here, on purpose.
# See the developer notes, Sobol' exploration.

import gzip
import math
import pickle
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from slurm_workflows.explore_space import ExplorationStudy, ExploreSpaceSobolQMC
from slurm_workflows.search_space import (
    CategoricalRange,
    FloatRange,
    IntRange,
    SearchSpace,
)
from slurm_workflows.slurm_pilot_executor import RaiseOnError, SlurmPilotExecutor, Task
from slurm_workflows.utils import RemoteExecutionError, gen_error_id
from worker_harness import make_worker, run_worker

SEED = 20260904

BOX_2D = {"x": FloatRange(-5.0, 5.0), "y": FloatRange(-5.0, 5.0)}

MIXED = {
    "x": FloatRange(-1.0, 1.0),
    "n": IntRange(2, 10),
    "kind": CategoricalRange(3),
}


def sphere(x: float, y: float) -> dict[str, float]:
    return {"objective": x * x + y * y}


def plane(x: float, y: float) -> dict[str, float]:
    return {"objective": x + y}


def mixed_objective(x: float, n: int, kind: int) -> dict[str, float]:
    return {"objective": abs(x) + n + kind, "n": n, "kind": kind}


class LocalExecutor:
    """Stands in for SlurmPilotExecutor, and runs each callable inline.

    Records the submissions it receives,
    and turns a raising objective into a `RemoteExecutionError` output,
    exactly as a real worker reports it.
    """

    def __init__(self) -> None:
        self.queues: list[str | list[str]] = []
        self.kwargs: list[dict] = []
        self.waits: list[str | None] = []
        self.batch_sizes: list[int] = []
        self.names: list[str] = []
        self.priorities: list[float] = []

    def submit(
        self,
        queue: str | list[str],
        fn: Callable[..., Any],
        *args,
        task_parents: list[Task] | None = None,
        task_priority: float = 0.0,
        **kwargs,
    ) -> Task:
        self.queues.append(queue)
        self.kwargs.append(dict(kwargs))
        self.priorities.append(task_priority)
        try:
            output = fn(*args, **kwargs)
        except Exception as e:
            output = RemoteExecutionError(str(e), gen_error_id())
        return Task(
            task_id=str(len(self.queues)),
            queue=[queue] if isinstance(queue, str) else list(queue),
            priority=task_priority,
            function=fn,
            input=(args, kwargs),
            output=output,
        )

    def set_task_name(self, task: Task, name: str) -> None:
        """Record a name, as the real executor does on the server."""
        self.names.append(name)
        task._task_name = name

    def wait(
        self,
        tasks: Sequence[Task],
        desc: str | None = None,
        unit: str = "task",
        raise_on_error: RaiseOnError | None = None,
    ) -> None:
        self.waits.append(desc)
        self.batch_sizes.append(len(tasks))
        # The exploration always waits with `RAISE_AFTER_COMPLETED`,
        # and every callable already ran in `submit`,
        # so raising on any failure is that policy here.
        failed = [t for t in tasks if isinstance(t.output, RemoteExecutionError)]
        if failed:
            raise RuntimeError(f"{len(failed)} of {len(tasks)} tasks did not succeed")


# The exploration makes the same three executor calls as the optimizer.
# See `as_executor` in `tests/test_optimize_space_botorch.py`.
# `TestRealExecutor` here checks that the three calls are enough.
# `exploration.executor` keeps the cast type,
# so a test that reads the stand-in's records back through it
# carries `# type: ignore[attr-defined]`.
def as_executor(executor: LocalExecutor) -> SlurmPilotExecutor:
    """Type the stand-in as the executor it stands in for."""
    return cast(SlurmPilotExecutor, executor)


def study(
    name: str = "demo",
    space: SearchSpace = BOX_2D,
    objective: Callable[..., Any] = sphere,
    queue: str | list[str] = "cpu",
    points: int | None = 8,
    seed: int | None = SEED,
    objective_key: str = "objective",
    priority: float = 0.0,
    **extra: Any,
) -> ExplorationStudy:
    """One exploration study, with the test defaults filled in."""
    return ExplorationStudy(
        name=name,
        space=space,
        objective=objective,
        objective_queue=queue,
        num_exploration_points=points,
        seed=seed,
        objective_key=objective_key,
        extra_objective_kwargs=extra,
        priority=priority,
    )


def explorer(
    *studies: ExplorationStudy, points: int | None = None
) -> ExploreSpaceSobolQMC:
    """An exploration of `studies`, or of one default study, on a stand-in executor."""
    return ExploreSpaceSobolQMC(
        list(studies) or [study()], as_executor(LocalExecutor()), points
    )


class TestConstruction:
    def test_the_point_count_is_floored_to_a_power_of_two(self):
        exploration = explorer(study(points=100))

        assert exploration.studies[0].num_exploration_points == 64

    def test_the_explorations_count_fills_in_for_a_study_without_one(self):
        exploration = explorer(study(points=None), points=16)

        assert exploration.studies[0].num_exploration_points == 16

    def test_a_studys_own_count_wins(self):
        exploration = explorer(study(points=4), points=64)

        assert exploration.studies[0].num_exploration_points == 4

    def test_a_count_from_neither_is_rejected(self):
        with pytest.raises(ValueError, match="num_exploration_points"):
            explorer(study(points=None))

    def test_a_point_count_below_one_is_rejected(self):
        with pytest.raises(ValueError):
            explorer(study(points=0))

    def test_no_studies_is_rejected(self):
        with pytest.raises(ValueError, match="no exploration studies"):
            ExploreSpaceSobolQMC([], as_executor(LocalExecutor()))

    def test_duplicate_task_names_are_rejected(self):
        """The name keys the results, so two studies of one name lose a result."""
        with pytest.raises(ValueError, match="unique"):
            explorer(study(name="a"), study(name="b"), study(name="a"))

    def test_an_empty_space_is_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            explorer(study(space={}))

    def test_extra_kwargs_may_not_shadow_a_parameter(self):
        with pytest.raises(ValueError, match="shadow"):
            explorer(study(x=1.0))

    def test_the_failing_task_is_named(self):
        with pytest.raises(ValueError, match="second"):
            explorer(study(name="first"), study(name="second", space={}))

    def test_a_missing_seed_is_drawn_and_printed(self, capsys):
        exploration = explorer(study(seed=None))

        assert isinstance(exploration.studies[0].seed, int)
        assert str(exploration.studies[0].seed) in capsys.readouterr().out

    def test_the_callers_task_is_left_alone(self):
        original = study(points=100, seed=None)

        exploration = explorer(original)

        assert original.num_exploration_points == 100
        assert original.seed is None
        assert exploration.studies[0].num_exploration_points == 64
        assert exploration.studies[0].seed is not None

    def test_nothing_is_submitted_until_it_runs(self):
        exploration = explorer()

        assert exploration.results["demo"].points == []
        with pytest.raises(RuntimeError, match="nothing has been evaluated"):
            exploration.best_point("demo")

    def test_an_unknown_task_name_says_what_there_is(self):
        exploration = explorer(study(name="a"))

        with pytest.raises(KeyError, match="'a'"):
            exploration.design("b")


class TestDesign:
    def test_it_draws_the_asked_for_number_of_points(self):
        assert len(explorer(study(points=8)).design("demo")) == 8

    def test_the_same_seed_draws_the_same_design(self):
        one = explorer(study(seed=7)).design("demo")
        two = explorer(study(seed=7)).design("demo")

        assert one == two

    def test_a_different_seed_draws_a_different_design(self):
        assert explorer(study(seed=7)).design("demo") != explorer(study(seed=8)).design(
            "demo"
        )

    def test_each_task_draws_in_its_own_space(self):
        exploration = explorer(
            study(name="box"),
            study(name="mixed", space=MIXED, objective=mixed_objective),
        )

        assert set(exploration.design("box")[0]) == {"x", "y"}
        assert set(exploration.design("mixed")[0]) == {"x", "n", "kind"}

    def test_every_point_is_inside_its_range(self):
        exploration = explorer(study(space=MIXED, objective=mixed_objective))

        for params in exploration.design("demo"):
            assert -1.0 <= params["x"] <= 1.0
            assert 2 <= params["n"] <= 10
            assert params["kind"] in (0, 1, 2)

    def test_discrete_parameters_come_back_as_integers(self):
        exploration = explorer(study(space=MIXED, objective=mixed_objective))

        params = exploration.design("demo")[0]

        assert isinstance(params["n"], int)
        assert isinstance(params["kind"], int)

    def test_the_design_covers_the_space_more_evenly_than_it_clumps(self):
        """Unlike a uniform draw, Sobol' puts exactly half the points in each half."""
        design = explorer(study(points=64)).design("demo")

        assert sum(1 for p in design if p["x"] < 0.0) == 32
        assert sum(1 for p in design if p["y"] < 0.0) == 32

    def test_drawing_does_not_evaluate(self):
        exploration = explorer()

        exploration.design("demo")

        assert exploration.results["demo"].values == []

    def test_the_dimension_is_the_tasks_own(self):
        exploration = explorer(
            study(name="box"),
            study(name="mixed", space=MIXED, objective=mixed_objective),
        )

        assert exploration.dim("box") == 2
        assert exploration.dim("mixed") == 3


class TestRun:
    def test_tasks_take_the_default_priority(self):
        executor = LocalExecutor()
        exploration = ExploreSpaceSobolQMC([study(points=4)], as_executor(executor))

        exploration.run()

        assert executor.priorities == [0.0] * 4

    def test_each_study_submits_at_its_own_priority(self):
        executor = LocalExecutor()
        exploration = ExploreSpaceSobolQMC(
            [study("low", points=2), study("high", points=2, priority=3.5)],
            as_executor(executor),
        )

        exploration.run()

        assert executor.priorities == [0.0, 0.0, 3.5, 3.5]

    def test_it_evaluates_every_point_of_the_design(self):
        exploration = explorer(study(points=8))

        exploration.run()

        result = exploration.results["demo"]
        assert len(result.points) == 8
        assert len(result.values) == 8
        assert len(result.outputs) == 8
        assert len(result.unit_points) == 8

    def test_the_values_are_the_objectives_own(self):
        exploration = explorer(study(points=8))

        exploration.run()

        result = exploration.results["demo"]
        for params, value in zip(result.points, result.values):
            assert math.isclose(value, sphere(**params)["objective"])

    def test_the_whole_output_is_kept_not_just_the_ranked_value(self):
        exploration = explorer(study(space=MIXED, objective=mixed_objective, points=4))

        exploration.run()

        outputs = exploration.results["demo"].outputs
        assert all(set(o) == {"objective", "n", "kind"} for o in outputs)

    def test_the_best_point_is_the_lowest_value(self):
        exploration = explorer(study(points=8))

        exploration.run()
        params, value = exploration.best_point("demo")

        result = exploration.results["demo"]
        assert value == min(result.values)
        assert params == result.points[result.values.index(value)]
        assert exploration.best_output("demo")["objective"] == value

    def test_the_best_of_each_task_is_reported(self, capsys):
        exploration = explorer(
            study(name="first", points=4), study(name="second", points=4)
        )

        exploration.run()

        out = capsys.readouterr().out
        assert "first: best of 4 points" in out
        assert "second: best of 4 points" in out

    def test_the_recorded_unit_point_is_the_one_evaluated(self):
        """The exploration rounds discrete parameters, and records the rounded point."""
        exploration = explorer(study(space=MIXED, objective=mixed_objective, points=4))

        exploration.run()

        result = exploration.results["demo"]
        for params, unit in zip(result.points, result.unit_points):
            assert math.isclose(unit[1], (params["n"] - 2) / (10 - 2))

    def test_extra_kwargs_reach_the_objective(self):
        def objective(x, y, scale):
            return {"objective": scale * (x * x + y * y)}

        exploration = explorer(study(objective=objective, points=4, scale=2.0))

        exploration.run()

        assert all("scale" in kw for kw in exploration.executor.kwargs)  # type: ignore[attr-defined]

    def test_a_second_run_repeats_the_design(self):
        """The seed decides the design, so this is a re-evaluation."""
        exploration = explorer(study(points=4))

        exploration.run()
        exploration.run()

        points = exploration.results["demo"].points
        assert points[:4] == points[4:]


class TestTaskNames:
    def test_every_point_is_named_after_its_task(self):
        exploration = explorer(study(points=4))

        exploration.run()

        assert exploration.executor.names == [  # type: ignore[attr-defined]
            "demo-explore-0",
            "demo-explore-1",
            "demo-explore-2",
            "demo-explore-3",
        ]

    def test_the_index_is_padded_so_the_names_sort(self):
        exploration = explorer(study(points=16))

        exploration.run()

        names = exploration.executor.names  # type: ignore[attr-defined]
        assert names[0] == "demo-explore-00"
        assert names[-1] == "demo-explore-15"
        assert names == sorted(names)

    def test_each_task_names_its_own_points(self):
        exploration = explorer(
            study(name="small", points=4),
            study(name="large", objective=plane, points=16),
        )

        exploration.run()

        names = exploration.executor.names  # type: ignore[attr-defined]
        assert sum(n.startswith("small-explore-") for n in names) == 4
        assert sum(n.startswith("large-explore-") for n in names) == 16

    def test_nothing_is_submitted_unnamed(self):
        exploration = explorer(study(points=8))

        exploration.run()

        executor = exploration.executor
        assert len(executor.names) == len(executor.queues)  # type: ignore[attr-defined]


class TestSeveralSpacesAtOnce:
    def test_every_task_is_evaluated(self):
        exploration = explorer(
            study(name="small", points=4),
            study(name="large", objective=plane, points=16),
        )

        exploration.run()

        assert len(exploration.results["small"].values) == 4
        assert len(exploration.results["large"].values) == 16

    def test_the_whole_exploration_is_submitted_before_any_of_it_is_waited_for(self):
        """The point of running them together: one batch, not one each."""
        exploration = explorer(
            study(name="small", points=4),
            study(name="large", objective=plane, points=16),
        )

        exploration.run()

        assert exploration.executor.batch_sizes == [20]  # type: ignore[attr-defined]
        assert len(exploration.executor.waits) == 1  # type: ignore[attr-defined]

    def test_each_task_goes_to_its_own_queue(self):
        exploration = explorer(
            study(name="on_cpu", queue="cpu", points=4),
            study(name="on_gpu", queue="gpu", objective=plane, points=4),
        )

        exploration.run()

        assert exploration.executor.queues == ["cpu"] * 4 + ["gpu"] * 4  # type: ignore[attr-defined]

    def test_results_are_kept_apart(self):
        exploration = explorer(
            study(name="sphere", points=4),
            study(name="plane", objective=plane, points=4),
        )

        exploration.run()

        for params, value in zip(
            exploration.results["plane"].points, exploration.results["plane"].values
        ):
            assert math.isclose(value, plane(**params)["objective"])
        assert all(v >= 0 for v in exploration.results["sphere"].values)

    def test_each_task_ranks_by_its_own_objective_key(self):
        exploration = explorer(
            study(name="a", points=4),
            study(
                name="b",
                objective=lambda x, y: {"loss": x * x + y * y},
                objective_key="loss",
                points=4,
            ),
        )

        exploration.run()

        assert len(exploration.results["b"].values) == 4

    def test_one_broken_task_names_itself(self):
        def boom(x, y):
            raise ValueError("objective blew up")

        exploration = explorer(
            study(name="fine", points=4),
            study(name="broken", objective=boom, points=4),
        )

        with pytest.raises(RuntimeError, match=r"exploration of \['broken'\]"):
            exploration.run()


class TestPartialFailure:
    """One bad point must not cost a whole exploration."""

    @staticmethod
    def fails_at(threshold: float) -> Callable[..., dict[str, float]]:
        """An objective that raises at every point whose `x` exceeds `threshold`."""

        def objective(x, y):
            if x > threshold:
                raise ValueError("objective blew up")
            return {"objective": x * x + y * y}

        return objective

    def test_the_points_that_came_back_are_kept(self):
        exploration = explorer(study(objective=self.fails_at(0.0), points=8))

        with pytest.raises(RuntimeError, match="objective evaluations failed"):
            exploration.run()

        result = exploration.results["demo"]
        assert result.values, "the successful points were thrown away"
        assert len(result.values) < 8
        assert all(x <= 0.0 for x in (p["x"] for p in result.points))

    def test_what_is_kept_is_index_aligned(self):
        exploration = explorer(study(objective=self.fails_at(0.0), points=8))

        with pytest.raises(RuntimeError):
            exploration.run()

        result = exploration.results["demo"]
        assert len(result.points) == len(result.values) == len(result.outputs)
        assert len(result.unit_points) == len(result.values)

    def test_a_saved_exploration_is_resumable_after_a_failure(self, tmp_path):
        """`save()` is what the next run reads, so it must not be empty."""
        exploration = explorer(study(objective=self.fails_at(0.0), points=8))

        with pytest.raises(RuntimeError):
            exploration.run()
        path: Path = tmp_path / "explore.pkl.gz"
        exploration.save(path)

        with gzip.open(path, "rb") as fobj:
            saved = pickle.load(fobj)
        assert saved["demo"]["values"] == exploration.results["demo"].values

    def test_a_task_whose_points_all_worked_keeps_all_of_them(self):
        """A failure in one study must not empty another's record."""
        exploration = explorer(
            study(name="fine", points=4),
            study(name="broken", objective=self.fails_at(-10.0), points=4),
        )

        with pytest.raises(RuntimeError, match=r"exploration of \['broken'\]"):
            exploration.run()

        assert len(exploration.results["fine"].values) == 4
        assert exploration.results["broken"].values == []


class TestSave:
    @staticmethod
    def load(path: Path):
        with gzip.open(path, "rb") as fobj:
            return pickle.load(fobj)

    def test_it_writes_what_each_task_measured(self, tmp_path):
        exploration = explorer(
            study(name="a", points=8), study(name="b", objective=plane, points=4)
        )
        exploration.run()

        exploration.save(tmp_path / "explore.pkl.gz")

        results = self.load(tmp_path / "explore.pkl.gz")
        assert sorted(results) == ["a", "b"]
        for name in ("a", "b"):
            assert results[name]["points"] == exploration.results[name].points
            assert results[name]["values"] == exploration.results[name].values
            assert results[name]["outputs"] == exploration.results[name].outputs

    def test_the_three_lists_stay_index_aligned(self, tmp_path):
        exploration = explorer(study(space=MIXED, objective=mixed_objective, points=4))
        exploration.run()

        exploration.save(tmp_path / "explore.pkl.gz")

        saved = self.load(tmp_path / "explore.pkl.gz")["demo"]
        for params, value, output in zip(
            saved["points"], saved["values"], saved["outputs"]
        ):
            assert output["objective"] == value
            assert output["n"] == params["n"]

    def test_the_file_is_gzipped(self, tmp_path):
        exploration = explorer(study(points=4))
        exploration.run()

        exploration.save(tmp_path / "explore.pkl.gz")

        assert (tmp_path / "explore.pkl.gz").read_bytes()[:2] == b"\x1f\x8b"

    def test_a_path_may_be_a_string(self, tmp_path):
        exploration = explorer(study(points=4))
        exploration.run()

        exploration.save(str(tmp_path / "explore.pkl.gz"))

        saved = self.load(tmp_path / "explore.pkl.gz")["demo"]
        assert saved["values"] == exploration.results["demo"].values

    def test_saving_again_replaces_the_file(self, tmp_path):
        exploration = explorer(study(points=4))
        exploration.run()
        exploration.save(tmp_path / "explore.pkl.gz")

        exploration.run()
        exploration.save(tmp_path / "explore.pkl.gz")

        assert len(self.load(tmp_path / "explore.pkl.gz")["demo"]["values"]) == 8

    def test_saving_before_running_writes_empty_lists(self, tmp_path):
        """The file says what the exploration measured, and that is nothing yet."""
        explorer(study(points=4)).save(tmp_path / "explore.pkl.gz")

        results = self.load(tmp_path / "explore.pkl.gz")
        assert results == {"demo": {"points": [], "values": [], "outputs": []}}


class TestObjectiveResults:
    def test_a_failed_evaluation_is_reported(self):
        def objective(x, y):
            raise ValueError("objective blew up")

        exploration = explorer(study(objective=objective, points=4))

        with pytest.raises(RuntimeError, match="objective evaluations failed"):
            exploration.run()

    def test_a_result_that_is_not_a_mapping_is_rejected(self):
        exploration = explorer(study(objective=lambda x, y: x + y, points=4))

        with pytest.raises(RuntimeError, match="expected a mapping"):
            exploration.run()

    def test_a_missing_objective_key_is_rejected(self):
        exploration = explorer(study(objective=lambda x, y: {"loss": x}, points=4))

        with pytest.raises(RuntimeError, match="no 'objective' among them"):
            exploration.run()

    def test_another_objective_key_can_be_named(self):
        exploration = explorer(
            study(
                objective=lambda x, y: {"loss": x * x + y * y},
                objective_key="loss",
                points=4,
            )
        )

        exploration.run()

        assert len(exploration.results["demo"].values) == 4

    def test_a_value_that_is_not_a_float_is_rejected(self):
        exploration = explorer(
            study(objective=lambda x, y: {"objective": "cheap"}, points=4)
        )

        with pytest.raises(RuntimeError, match="not a float"):
            exploration.run()

    def test_a_non_finite_value_is_rejected(self):
        exploration = explorer(
            study(objective=lambda x, y: {"objective": float("nan")}, points=4)
        )

        with pytest.raises(RuntimeError, match="non-finite"):
            exploration.run()


class TestRealExecutor:
    """One end-to-end exploration against the real server, executor and worker."""

    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs):
        pilot_jobs("cpu")

    def test_it_explores_two_spaces_through_a_real_worker(
        self, executor, ds_service_address, tmp_path
    ):
        points = 4
        exploration = ExploreSpaceSobolQMC(
            [
                ExplorationStudy("box", BOX_2D, sphere, "cpu", points, SEED),
                ExplorationStudy("flat", BOX_2D, plane, "cpu", points, SEED),
            ],
            executor,
        )

        # Why a real worker runs in a thread:
        # see docs/how-to-run-tests.md, "Notes for future changes".
        worker = make_worker(ds_service_address, tmp_path / "worker", group="cpu")
        thread = threading.Thread(
            target=run_worker, args=(worker, 2 * points), daemon=True
        )
        thread.start()
        try:
            exploration.run()
        finally:
            thread.join(timeout=30)
            worker.close()

        assert not thread.is_alive(), "worker thread did not finish"
        for name in ("box", "flat"):
            result = exploration.results[name]
            assert len(result.values) == points
            assert exploration.best_point(name)[1] == min(result.values)

    def test_the_names_it_gives_are_on_the_queue_server(
        self, executor, ds_service_address, ds_client, tmp_path
    ):
        """What `swtop` reads: the names, keyed by task id, in the map."""
        points = 4
        exploration = ExploreSpaceSobolQMC(
            [
                ExplorationStudy("box", BOX_2D, sphere, "cpu", points, SEED),
                ExplorationStudy("flat", BOX_2D, plane, "cpu", points, SEED),
            ],
            executor,
        )

        worker = make_worker(ds_service_address, tmp_path / "worker", group="cpu")
        thread = threading.Thread(
            target=run_worker, args=(worker, 2 * points), daemon=True
        )
        thread.start()
        try:
            exploration.run()
        finally:
            thread.join(timeout=30)
            worker.close()

        stored = sorted(
            ds_client.map_get(key).decode("utf-8")
            for key in ds_client.map_search_key("^task_name:")
        )
        assert stored == [
            f"{name}-explore-{i}" for name in ("box", "flat") for i in range(points)
        ]
