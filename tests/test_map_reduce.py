"""Tests for `SlurmPilotExecutor.map_reduce`."""

from __future__ import annotations

import json
import operator
import threading
from typing import Callable, NoReturn

import pytest
import cloudpickle
from typeguard import TypeCheckError
from ds_service_client import TaskState

from slurm_workflows import slurm_pilot_executor
from slurm_workflows.slurm_pilot_executor import (
    MAP_REDUCE_TOKEN_LEN,
    SlurmPilotExecutor,
    _map_reduce_task,
    _reduce_task,
)
from slurm_workflows.slurm_pilot_worker import current_actor
from slurm_workflows.swtop import ALL_TASK_IDS

import support_actor

from support_map import RecordingClient, item_ids

# --------------------------------------------------------------------------
# What a call is made of
# --------------------------------------------------------------------------


def identity[T](x: T) -> T:
    return x


def add(acc, x):
    return acc + x


def scale(x: int, factor: int, offset: int = 0) -> int:
    return x * factor + offset


def add_mod(acc: int, x: int, modulus: int = 1000) -> int:
    """An associative `reduce_fn` that takes an extra keyword argument."""
    return (acc + x) % modulus


def wrap[T](x: T) -> list[T]:
    """Map one item to a one-item list, so `add` concatenates."""
    return [x]


def extend_in_place(acc: list[int], x: list[int]) -> list[int]:
    """A `reduce_fn` that folds into its accumulator rather than replace it."""
    acc.extend(x)
    return acc


def explode(x: int) -> NoReturn:
    raise ValueError(f"no good: {x}")


def count_hits(path: str, threshold: float) -> int:
    """A map function that counts the rows of one file above a threshold."""
    with open(path) as fobj:
        return sum(1 for line in fobj if float(line.split(",")[2]) > threshold)


# --------------------------------------------------------------------------
# results
# --------------------------------------------------------------------------


class TestResults:
    def test_it_sums_a_range(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=5)

        total = executor.map_reduce(
            desc="sum",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(100),
            init=0,
            num_tasks=4,
        )

        assert total == 4950

    def test_every_item_is_mapped_once(self, executor, pilot_jobs, worker_thread):
        """A fold that keeps the items shows a lost one and a doubled one."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        got = executor.map_reduce(
            desc="collect",
            queue="cpu",
            map_fn=wrap,
            reduce_fn=add,
            iterable=range(50),
            init=[],
            num_tasks=3,
        )

        assert sorted(got) == list(range(50))

    def test_it_passes_the_extra_arguments(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        total = executor.map_reduce(
            desc="weighted",
            queue="cpu",
            map_fn=scale,
            reduce_fn=add_mod,
            iterable=range(10),
            init=0,
            num_tasks=2,
            map_extra_args=(3,),
            map_extra_kwargs={"offset": 1},
            reduce_extra_args=(),
            reduce_extra_kwargs={"modulus": 100},
        )

        assert total == sum(x * 3 + 1 for x in range(10)) % 100

    def test_one_task_agrees_with_many(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=(1 + 1) + (8 + 1))

        one = executor.map_reduce(
            desc="one",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(20),
            init=0,
            num_tasks=1,
        )
        many = executor.map_reduce(
            desc="many",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(20),
            init=0,
            num_tasks=8,
        )

        assert one == many == 190

    def test_a_queue_list_works(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        total = executor.map_reduce(
            desc="sum",
            queue=["cpu"],
            map_fn=identity,
            reduce_fn=add,
            iterable=range(10),
            init=0,
            num_tasks=2,
        )

        assert total == 45

    def test_it_does_not_mutate_init(self, executor, pilot_jobs, worker_thread):
        """A `reduce_fn` that folds in place leaves the caller's value alone."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)
        init: list[int] = []

        got = executor.map_reduce(
            desc="collect",
            queue="cpu",
            map_fn=wrap,
            reduce_fn=extend_in_place,
            iterable=range(10),
            init=init,
            num_tasks=2,
        )

        assert sorted(got) == list(range(10))
        assert init == []

    def test_the_documented_shape_works(
        self, executor, pilot_jobs, worker_thread, tmp_path
    ):
        """A count of matching rows across files, with an extra map argument."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=5)

        paths = []
        for index in range(20):
            path = tmp_path / f"rows-{index}.csv"
            path.write_text("".join(f"a,b,{i}.0\n" for i in range(index)))
            paths.append(str(path))

        hits = executor.map_reduce(
            desc="scanning",
            queue="cpu",
            map_fn=count_hits,
            reduce_fn=operator.add,
            iterable=paths,
            init=0,
            num_tasks=4,
            map_extra_args=(1.5,),
        )

        assert hits == sum(max(index - 2, 0) for index in range(20))

    def test_the_documented_gather_shape_works(
        self, executor, pilot_jobs, worker_thread
    ):
        """A map to a one-item list, then a concatenation.

        The shape is the one docs/reference/map-reduce.md shows.
        """
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        got = executor.map_reduce(
            desc="gather",
            queue="cpu",
            map_fn=lambda x: [x * x],
            reduce_fn=operator.add,
            iterable=range(10),
            init=[],
            num_tasks=2,
        )

        assert sorted(got) == [x * x for x in range(10)]


# --------------------------------------------------------------------------
# actors
# --------------------------------------------------------------------------


class TestActors:
    """`map_fn` as the name of a method on the job group's actor."""

    @pytest.fixture
    def actor_group(self, executor) -> None:
        """A job group whose workers build a `MapActor` with `factor=3`."""
        executor.define_job_group(
            "act",
            [],
            actor_class_name="support_actor.MapActor",
            actor_class_args=[3],
        )
        executor.scale_jobs("act", 1)

    def test_a_method_name_maps_on_the_actor(
        self, executor, actor_group, worker_thread
    ):
        worker_thread(
            expect_tasks=3, group="act", actor_class_name="support_actor.MapActor"
        )

        total = executor.map_reduce(
            desc="scale",
            queue="act",
            map_fn="scale",
            reduce_fn=add,
            iterable=range(10),
            init=0,
            num_tasks=2,
        )

        assert total == 3 * sum(range(10))

    def test_the_actor_is_the_one_the_worker_built(
        self, executor, actor_group, worker_thread
    ):
        """The whole point: one actor per worker, not one per item."""
        support_actor.INSTANCES.clear()
        worker_thread(
            expect_tasks=2, group="act", actor_class_name="support_actor.MapActor"
        )

        executor.map_reduce(
            desc="scale",
            queue="act",
            map_fn="scale",
            reduce_fn=add,
            iterable=range(6),
            init=0,
            num_tasks=1,
        )

        # The worker runs in a thread of this process,
        # and `current_actor()` is process-wide.
        actor = current_actor()
        assert isinstance(actor, support_actor.MapActor)
        assert actor.calls == 6

    def test_map_extra_args_reach_the_method(
        self, executor, actor_group, worker_thread
    ):
        worker_thread(
            expect_tasks=2, group="act", actor_class_name="support_actor.MapActor"
        )

        total = executor.map_reduce(
            desc="offset",
            queue="act",
            map_fn="offset",
            reduce_fn=add,
            iterable=range(5),
            init=0,
            num_tasks=1,
            map_extra_args=(2,),
            map_extra_kwargs={"sign": -1},
        )

        assert total == sum(-(x * 3 + 2) for x in range(5))

    def test_a_failing_method_raises(self, executor, actor_group, worker_thread):
        worker_thread(
            expect_tasks=1, group="act", actor_class_name="support_actor.MapActor"
        )

        with pytest.raises(RuntimeError, match="failed on its worker"):
            executor.map_reduce(
                desc="boom",
                queue="act",
                map_fn="explode",
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=1,
            )

    def test_a_callable_still_runs_on_an_actor_group(
        self, executor, actor_group, worker_thread
    ):
        """The actor is there for a method name, and does not block a callable."""
        worker_thread(
            expect_tasks=3, group="act", actor_class_name="support_actor.MapActor"
        )

        total = executor.map_reduce(
            desc="sum",
            queue="act",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(10),
            init=0,
            num_tasks=2,
        )

        assert total == 45


# --------------------------------------------------------------------------
# the worker-side function, driven directly
# --------------------------------------------------------------------------


class TestMapReduceTask:
    def test_concurrent_tasks_split_the_queue(
        self, ds_client, ds_service_address, map_task_env
    ):
        """Three tasks on one queue fold every item exactly once between them."""
        queue = "mr-direct"
        for index in range(60):
            # Any priority will do: these tests do not depend on serving order.
            ds_client.task_add(
                task_id=f"{queue}.item.{index}",
                parent_task_ids=[],
                queue=[queue],
                priority=float(-index),
                function=b"",
                input=cloudpickle.dumps(index),
            )

        partials: list[list[int]] = []
        lock = threading.Lock()

        def fold():
            got = _map_reduce_task(queue, wrap, add, [], (), {}, (), {})
            with lock:
                partials.append(got)

        threads = [threading.Thread(target=fold) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not any(t.is_alive() for t in threads)
        assert sorted(i for partial in partials for i in partial) == list(range(60))

    def test_it_marks_each_item_done(self, ds_client, ds_service_address, map_task_env):
        queue = "mr-done"
        for index in range(5):
            # Any priority will do: these tests do not depend on serving order.
            ds_client.task_add(
                task_id=f"{queue}.item.{index}",
                parent_task_ids=[],
                queue=[queue],
                priority=float(-index),
                function=b"",
                input=cloudpickle.dumps(index),
            )

        assert _map_reduce_task(queue, identity, add, 0, (), {}, (), {}) == 10

        for index in range(5):
            task_id = f"{queue}.item.{index}"
            assert ds_client.task_get_status(task_id) == TaskState.Finished
            # See the developer notes, "map_reduce and map".
            assert ds_client.task_get_output(task_id) == b""

    def test_an_empty_queue_returns_init(self, ds_service_address, map_task_env):
        assert _map_reduce_task("mr-empty", identity, add, 7, (), {}, (), {}) == 7


# --------------------------------------------------------------------------
# queues, ids and ordering
# --------------------------------------------------------------------------


class TestQueuesAndIds:
    def test_every_item_is_enqueued_before_the_first_task(
        self, executor, pilot_jobs, worker_thread
    ):
        """The invariant a task's "queue is empty" answer rests on."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)
        recorder = RecordingClient(executor.client)
        executor.client = recorder

        executor.map_reduce(
            desc="sum",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(10),
            init=0,
            num_tasks=2,
        )

        items = [i for i, tid in enumerate(recorder.added_ids) if ".item." in tid]
        tasks = [i for i, tid in enumerate(recorder.added_ids) if ".task." in tid]
        assert len(items) == 10
        # Two map tasks and the reduce task.
        assert len(tasks) == 3
        assert max(items) < min(tasks)

    def test_item_ids_do_not_collide_with_task_ids(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        executor.map_reduce(
            desc="sum",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(6),
            init=0,
            num_tasks=2,
        )

        items = item_ids(ds_client)
        assert len(items) == 6
        prefix = f"{executor.name}.map_reduce.0."
        assert all(i.startswith(prefix) for i in items)
        # `<name>.map_reduce.<index>.<token>.item.<i>`, and the token is hex.
        token = items[0][len(prefix) :].split(".")[0]
        assert len(token) == MAP_REDUCE_TOKEN_LEN
        assert int(token, 16) >= 0

        # The submitted tasks kept the plain numbering, and nothing collided.
        assert executor.submit("cpu", identity, 1).task_id == f"{executor.name}.task.3"

    def test_two_calls_use_two_queues(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        for _ in range(2):
            executor.map_reduce(
                desc="sum",
                queue="cpu",
                map_fn=identity,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=1,
            )

        queues = {i.split(".item.")[0] for i in item_ids(ds_client)}
        assert len(queues) == 2
        # A queue is `<name>.map_reduce.<index>.<token>`, so field 2 is the index.
        assert {q.split(".")[2] for q in queues} == {"0", "1"}

    def test_it_publishes_progress(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        executor.map_reduce(
            desc="counting things",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(30),
            init=0,
            num_tasks=3,
        )

        display = json.loads(ds_client.map_get("progress_display"))
        assert display["desc"] == "counting things"
        assert display["unit"] == "task"
        # Three map tasks and the reduce task.
        assert display["total"] == 4

    def test_it_names_its_tasks(self, executor, ds_client, pilot_jobs, worker_thread):
        """What `swtop` shows for a map_reduce task."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        executor.map_reduce(
            desc="sum",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(4),
            init=0,
            num_tasks=2,
        )

        names = {
            ds_client.map_get(f"task_name:{executor.name}.task.{i}").decode("utf-8")
            for i in range(3)
        }
        (queue,) = {j.split(".item.")[0] for j in item_ids(ds_client)}
        assert names == {f"{queue}.task.0", f"{queue}.task.1", f"{queue}.reduce"}


# --------------------------------------------------------------------------
# edge cases and refusals
# --------------------------------------------------------------------------


class TestEdgeCases:
    def test_an_empty_iterable_returns_init(self, executor, ds_client):
        """No worker, no queue, no task: the call never waits."""
        assert (
            executor.map_reduce(
                desc="nothing",
                queue="cpu",
                map_fn=identity,
                reduce_fn=add,
                iterable=[],
                init=0,
                num_tasks=4,
            )
            == 0
        )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []
        assert executor.next_map_reduce_index == 0

    def test_an_empty_iterable_copies_init(self, executor):
        init: list[int] = []
        got = executor.map_reduce(
            desc="nothing",
            queue="cpu",
            map_fn=wrap,
            reduce_fn=extend_in_place,
            iterable=[],
            init=init,
            num_tasks=1,
        )
        assert got == []
        assert got is not init

    def test_fewer_items_than_tasks(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        """`num_tasks` is an upper bound, and each map task gets at least one item."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        total = executor.map_reduce(
            desc="sum",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=[1, 2, 3],
            init=0,
            num_tasks=8,
        )

        assert total == 6
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        # The map tasks and the reduce task.
        assert len(submitted) == 4

    def test_num_tasks_below_one_raises(self, executor, ds_client):
        with pytest.raises(ValueError, match="num_tasks"):
            executor.map_reduce(
                desc="sum",
                queue="cpu",
                map_fn=identity,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=0,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_non_int_num_tasks_raises(self, executor):
        with pytest.raises(TypeCheckError):
            executor.map_reduce(
                desc="sum",
                queue="cpu",
                map_fn=identity,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks="4",
            )

    def test_a_method_name_needs_an_actor(self, executor, pilot_jobs, ds_client):
        """Only a job group with an actor can resolve a method name."""
        pilot_jobs("cpu")
        with pytest.raises(ValueError, match="actor"):
            executor.map_reduce(
                desc="sum",
                queue="cpu",
                map_fn="scale",
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=2,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_method_name_on_a_worker_without_an_actor_is_reported(
        self, ds_service_address, map_task_env
    ):
        """The worker-side half of the same check, for a group defined elsewhere."""
        with pytest.raises(RuntimeError, match="no actor"):
            _map_reduce_task("mr-no-actor", "scale", add, 0, (), {}, (), {})

    def test_a_queue_with_no_worker_raises_before_enqueueing(self, executor, ds_client):
        with pytest.raises(RuntimeError, match="no worker started"):
            executor.map_reduce(
                desc="sum",
                queue="cpu",
                map_fn=identity,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=2,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_failing_map_fn_raises(self, executor, pilot_jobs, worker_thread):
        """The worker turns it into a `RemoteExecutionError`, and the wait raises."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=1)

        with pytest.raises(RuntimeError, match="failed on its worker"):
            executor.map_reduce(
                desc="boom",
                queue="cpu",
                map_fn=explode,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=1,
            )


# --------------------------------------------------------------------------
# the reduce task
# --------------------------------------------------------------------------


class TestReduceTask:
    def run_sum(
        self,
        executor,
        pilot_jobs: Callable[..., None],
        worker_thread: Callable[..., None],
    ) -> RecordingClient:
        """Sum a range with two map tasks, and record the driver's calls."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)
        recorder = RecordingClient(executor.client)
        executor.client = recorder

        total = executor.map_reduce(
            "cpu", identity, add, range(10), 0, num_tasks=2, desc="sum"
        )

        assert total == 45
        return recorder

    def test_it_depends_on_every_map_task(self, executor, pilot_jobs, worker_thread):
        recorder = self.run_sum(executor, pilot_jobs, worker_thread)

        tasks = [tid for tid in recorder.added_ids if ".task." in tid]
        *map_tasks, reduce_task = tasks
        assert len(map_tasks) == 2
        assert recorder.parent_ids[reduce_task] == map_tasks
        assert all(recorder.parent_ids[tid] == [] for tid in map_tasks)

    def test_the_driver_reads_only_its_output(
        self, executor, pilot_jobs, worker_thread
    ):
        """The partial results stay on the server, and never reach the driver."""
        recorder = self.run_sum(executor, pilot_jobs, worker_thread)

        reduce_task = [tid for tid in recorder.added_ids if ".task." in tid][-1]
        assert recorder.output_reads == [reduce_task]

    def test_it_folds_the_partial_results_in_map_task_order(
        self, ds_client, ds_service_address, map_task_env
    ):
        """Called directly, with the map task outputs already on the server."""
        for index, partial in enumerate([[1], [2, 3], [4]]):
            task_id = f"mr.task.{index}"
            ds_client.task_add(
                task_id=task_id,
                parent_task_ids=[],
                queue=["q"],
                priority=0.0,
                function=b"",
                input=b"",
            )
            claimed = ds_client.task_get("w", "q")
            assert claimed.task_id == task_id
            ds_client.task_done(task_id, "w", cloudpickle.dumps(partial))

        got = _reduce_task(["mr.task.2", "mr.task.0", "mr.task.1"], add, [], (), {})

        assert got == [4, 1, 2, 3]

    def test_a_failed_map_task_is_reported_with_its_own_error(
        self, executor, pilot_jobs, worker_thread
    ):
        """Not as the dependency failure of the reduce task."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=1)

        with pytest.raises(RuntimeError, match="no good"):
            executor.map_reduce("cpu", explode, add, range(4), 0, num_tasks=1)


# --------------------------------------------------------------------------
# defaults
# --------------------------------------------------------------------------


class TestDefaults:
    def test_queue_functions_iterable_and_init_are_enough(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        """The short form: one map task per item, and a generated label."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=6)

        total = executor.map_reduce("cpu", identity, add, range(5), 0)

        assert total == 10
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        # The map tasks and the reduce task.
        assert len(submitted) == 6
        display = json.loads(ds_client.map_get("progress_display"))
        assert display["desc"] == "map_reduce-0"

    def test_the_default_task_count_is_bounded(
        self, executor, ds_client, pilot_jobs, worker_thread, monkeypatch
    ):
        monkeypatch.setattr(slurm_pilot_executor, "DEFAULT_MAP_TASKS", 2)
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        total = executor.map_reduce("cpu", identity, add, range(10), 0)

        assert total == 45
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        # The map tasks and the reduce task.
        assert len(submitted) == 3

    def test_num_tasks_and_desc_are_keyword_only(self, executor):
        with pytest.raises(TypeError):
            executor.map_reduce("cpu", identity, add, range(4), 0, 2)
