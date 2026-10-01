"""Tests for `SlurmPilotExecutor.map`."""

from __future__ import annotations

import json
import threading
from typing import Any, NoReturn

import pytest
import cloudpickle
from ds_service_client import DsServiceClient, TaskState

from slurm_workflows import slurm_pilot_executor
from slurm_workflows.slurm_pilot_executor import MAP_REDUCE_TOKEN_LEN, _map_task
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


def square(x: int) -> int:
    return x * x


def scale(x: int, factor: int, offset: int = 0) -> int:
    return x * factor + offset


def explode(x: int) -> NoReturn:
    raise ValueError(f"no good: {x}")


def add_items(ds_client: DsServiceClient, queue: str, count: int) -> None:
    """Put `count` items on `queue`, the way `map` does."""
    for index in range(count):
        ds_client.task_add(
            task_id=f"{queue}.item.{index}",
            parent_task_ids=[],
            queue=[queue],
            priority=0.0,
            function=b"",
            input=cloudpickle.dumps(index),
        )


# --------------------------------------------------------------------------
# results
# --------------------------------------------------------------------------


class TestResults:
    def test_it_maps_a_range_in_order(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        got = executor.map(
            desc="square",
            queue="cpu",
            map_fn=square,
            iterable=range(100),
            num_tasks=4,
        )

        assert got == [x * x for x in range(100)]

    def test_the_order_does_not_rest_on_the_task_output_order(
        self, executor, pilot_jobs, worker_thread, monkeypatch
    ):
        """The driver places each value by its item id, not by where it came back."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)
        real_map_task = slurm_pilot_executor._map_task

        def reversed_map_task(*args: Any) -> list[tuple[str, Any]]:
            return list(reversed(real_map_task(*args)))

        # The worker runs in a thread of this process,
        # so it runs whatever the executor submits.
        monkeypatch.setattr(slurm_pilot_executor, "_map_task", reversed_map_task)

        got = executor.map(
            desc="square",
            queue="cpu",
            map_fn=square,
            iterable=range(20),
            num_tasks=2,
        )

        assert got == [x * x for x in range(20)]

    def test_it_passes_the_extra_arguments(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        got = executor.map(
            desc="weighted",
            queue="cpu",
            map_fn=scale,
            iterable=range(10),
            num_tasks=2,
            map_extra_args=(3,),
            map_extra_kwargs={"offset": 1},
        )

        assert got == [x * 3 + 1 for x in range(10)]

    def test_a_queue_list_needs_only_one_started_group(
        self, executor, pilot_jobs, worker_thread
    ):
        """A job group with no pilot job in the list does not refuse the call."""
        pilot_jobs("cpu")
        executor.define_job_group("idle", [])
        worker_thread(expect_tasks=2)

        got = executor.map(
            desc="square",
            queue=["cpu", "idle"],
            map_fn=square,
            iterable=range(10),
            num_tasks=2,
        )

        assert got == [x * x for x in range(10)]

    def test_a_generator_is_read_in_full_and_in_order(
        self, executor, pilot_jobs, worker_thread
    ):
        """A one-shot iterable gives up each item once, so it must be read once."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        got = executor.map(
            desc="square",
            queue="cpu",
            map_fn=square,
            iterable=(x for x in range(20)),
            num_tasks=3,
        )

        assert got == [x * x for x in range(20)]

    def test_a_none_value_is_a_value(self, executor, pilot_jobs, worker_thread):
        """`None` from `map_fn` comes back as `None`, and does not look missing."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=1)

        got = executor.map(
            desc="nothing",
            queue="cpu",
            map_fn=lambda x: None,
            iterable=range(3),
            num_tasks=1,
        )

        assert got == [None, None, None]


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
            expect_tasks=2, group="act", actor_class_name="support_actor.MapActor"
        )

        got = executor.map(
            desc="scale", queue="act", map_fn="scale", iterable=range(10), num_tasks=2
        )

        assert got == [3 * x for x in range(10)]

    def test_the_actor_is_the_one_the_worker_built(
        self, executor, actor_group, worker_thread
    ):
        """The whole point: one actor per worker, not one per item."""
        worker_thread(
            expect_tasks=1, group="act", actor_class_name="support_actor.MapActor"
        )

        executor.map(
            desc="scale", queue="act", map_fn="scale", iterable=range(6), num_tasks=1
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
            expect_tasks=1, group="act", actor_class_name="support_actor.MapActor"
        )

        got = executor.map(
            desc="offset",
            queue="act",
            map_fn="offset",
            iterable=range(5),
            num_tasks=1,
            map_extra_args=(2,),
            map_extra_kwargs={"sign": -1},
        )

        assert got == [-(x * 3 + 2) for x in range(5)]

    def test_a_callable_still_runs_on_an_actor_group(
        self, executor, actor_group, worker_thread
    ):
        """The actor is there for a method name, and does not block a callable."""
        worker_thread(
            expect_tasks=2, group="act", actor_class_name="support_actor.MapActor"
        )

        got = executor.map(
            desc="square", queue="act", map_fn=square, iterable=range(10), num_tasks=2
        )

        assert got == [x * x for x in range(10)]


# --------------------------------------------------------------------------
# the worker-side function, driven directly
# --------------------------------------------------------------------------


class TestMapTask:
    def test_concurrent_tasks_split_the_queue(
        self, ds_client, ds_service_address, map_task_env
    ):
        """Three tasks on one queue map every item exactly once between them."""
        queue = "map-direct"
        add_items(ds_client, queue, 60)

        pairs: list[tuple[str, Any]] = []
        lock = threading.Lock()

        def run():
            got = _map_task(queue, identity, (), {})
            with lock:
                pairs.extend(got)

        threads = [threading.Thread(target=run) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not any(t.is_alive() for t in threads)
        assert sorted(pairs, key=lambda pair: pair[1]) == [
            (f"{queue}.item.{index}", index) for index in range(60)
        ]

    def test_it_marks_each_item_done(self, ds_client, ds_service_address, map_task_env):
        queue = "map-done"
        add_items(ds_client, queue, 5)

        got = _map_task(queue, square, (), {})

        assert sorted(got) == [(f"{queue}.item.{i}", i * i) for i in range(5)]
        for index in range(5):
            task_id = f"{queue}.item.{index}"
            assert ds_client.task_get_status(task_id) == TaskState.Finished
            # See the developer notes, "map_reduce and map".
            assert ds_client.task_get_output(task_id) == b""

    def test_an_empty_queue_returns_nothing(self, ds_service_address, map_task_env):
        assert _map_task("map-empty", identity, (), {}) == []

    def test_a_method_name_on_a_worker_without_an_actor_is_reported(
        self, ds_service_address, map_task_env
    ):
        """The worker-side half of the actor check, for a group defined elsewhere."""
        with pytest.raises(RuntimeError, match="map map_fn .* no actor"):
            _map_task("map-no-actor", "scale", (), {})


# --------------------------------------------------------------------------
# queues, ids and ordering
# --------------------------------------------------------------------------


class TestQueuesAndIds:
    def test_every_item_is_enqueued_before_the_first_task(
        self, executor, pilot_jobs, worker_thread
    ):
        """The invariant a task's "queue is empty" answer rests on."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)
        recorder = RecordingClient(executor.client)
        executor.client = recorder

        executor.map(
            desc="square", queue="cpu", map_fn=square, iterable=range(10), num_tasks=2
        )

        items = [i for i, tid in enumerate(recorder.added_ids) if ".item." in tid]
        tasks = [i for i, tid in enumerate(recorder.added_ids) if ".task." in tid]
        assert len(items) == 10
        assert len(tasks) == 2
        assert max(items) < min(tasks)

    def test_map_and_map_reduce_use_queues_of_their_own(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        executor.map(
            desc="square", queue="cpu", map_fn=square, iterable=range(4), num_tasks=1
        )
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
        # A queue is `<name>.<kind>.<index>.<token>`.
        assert {tuple(q.split(".")[1:3]) for q in queues} == {
            ("map", "0"),
            ("map_reduce", "0"),
        }

    def test_it_publishes_progress_task_names_and_distinct_item_ids(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        """What `swtop` shows for a call, and the ids it puts on the server."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        executor.map(
            desc="squaring things",
            queue="cpu",
            map_fn=square,
            iterable=range(6),
            num_tasks=3,
        )

        display = json.loads(ds_client.map_get("progress_display"))
        assert display["desc"] == "squaring things"
        assert display["unit"] == "task"
        assert display["total"] == 3

        items = item_ids(ds_client)
        assert len(items) == 6
        prefix = f"{executor.name}.map.0."
        assert all(i.startswith(prefix) for i in items)
        # `<name>.map.<index>.<token>.item.<i>`, and the token is hex.
        token = items[0][len(prefix) :].split(".")[0]
        assert len(token) == MAP_REDUCE_TOKEN_LEN
        assert int(token, 16) >= 0

        names = {
            ds_client.map_get(f"task_name:{executor.name}.task.{i}").decode("utf-8")
            for i in range(3)
        }
        assert names == {f"{prefix}{token}.task.{i}" for i in range(3)}

        # The submitted tasks kept the plain numbering, and nothing collided.
        assert executor.submit("cpu", identity, 1).task_id == f"{executor.name}.task.3"


# --------------------------------------------------------------------------
# edge cases and refusals
# --------------------------------------------------------------------------


class TestEdgeCases:
    def test_an_empty_iterable_returns_an_empty_list(self, executor, ds_client):
        """No worker, no queue, no task: the call never waits."""
        got = executor.map(
            desc="nothing", queue="cpu", map_fn=square, iterable=[], num_tasks=4
        )
        assert got == []
        assert ds_client.task_search_id(ALL_TASK_IDS) == []
        assert executor.next_map_index == 0

    def test_fewer_items_than_tasks(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        """`num_tasks` is an upper bound: the call submits no task without an item."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        got = executor.map(
            desc="square", queue="cpu", map_fn=square, iterable=[1, 2, 3], num_tasks=8
        )

        assert got == [1, 4, 9]
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        assert len(submitted) == 3

    def test_num_tasks_below_one_raises(self, executor, ds_client):
        with pytest.raises(ValueError, match="num_tasks"):
            executor.map(
                desc="square",
                queue="cpu",
                map_fn=square,
                iterable=range(4),
                num_tasks=0,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_method_name_needs_an_actor(self, executor, pilot_jobs, ds_client):
        """Only a job group with an actor can resolve a method name."""
        pilot_jobs("cpu")
        with pytest.raises(ValueError, match="map names a method"):
            executor.map(
                desc="scale",
                queue="cpu",
                map_fn="scale",
                iterable=range(4),
                num_tasks=2,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_method_name_names_the_group_without_an_actor(self, executor, ds_client):
        """In a queue list, one job group with no actor is enough to refuse."""
        executor.define_job_group("act", [], actor_class_name="support_actor.MapActor")
        executor.define_job_group("cpu", [])

        with pytest.raises(ValueError, match=r"map names a method.*: \['cpu'\]$"):
            executor.map(
                desc="scale",
                queue=["act", "cpu"],
                map_fn="scale",
                iterable=range(4),
                num_tasks=2,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_queue_with_no_worker_raises_before_enqueueing(self, executor, ds_client):
        with pytest.raises(RuntimeError, match="no worker started"):
            executor.map(
                desc="square",
                queue="cpu",
                map_fn=square,
                iterable=range(4),
                num_tasks=2,
            )
        assert ds_client.task_search_id(ALL_TASK_IDS) == []

    def test_a_failing_map_fn_raises(self, executor, pilot_jobs, worker_thread):
        """The worker turns it into a `RemoteExecutionError`, and the wait raises."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=1)

        with pytest.raises(RuntimeError, match="failed on its worker"):
            executor.map(
                desc="boom", queue="cpu", map_fn=explode, iterable=range(4), num_tasks=1
            )

    def test_a_missing_value_raises(
        self, executor, pilot_jobs, worker_thread, monkeypatch
    ):
        """A broken ordering appears as an error, never as a gap in the list."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=1)
        real_map_task = slurm_pilot_executor._map_task

        def lossy_map_task(*args: Any) -> list[tuple[str, Any]]:
            return real_map_task(*args)[1:]

        monkeypatch.setattr(slurm_pilot_executor, "_map_task", lossy_map_task)

        with pytest.raises(RuntimeError, match="1 of 4 items .* no value"):
            executor.map(
                desc="lossy", queue="cpu", map_fn=square, iterable=range(4), num_tasks=1
            )


# --------------------------------------------------------------------------
# defaults
# --------------------------------------------------------------------------


class TestDefaults:
    def test_queue_function_and_iterable_are_enough(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        """The short form: one task per item, and a generated label."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=5)

        got = executor.map("cpu", square, range(5))

        assert got == [x * x for x in range(5)]
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        assert len(submitted) == 5
        display = json.loads(ds_client.map_get("progress_display"))
        assert display["desc"] == "map-0"

    def test_the_default_label_counts_the_calls(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        executor.map("cpu", square, [1])
        executor.map("cpu", square, [2])

        display = json.loads(ds_client.map_get("progress_display"))
        assert display["desc"] == "map-1"

    def test_the_default_task_count_is_bounded(
        self, executor, ds_client, pilot_jobs, worker_thread, monkeypatch
    ):
        monkeypatch.setattr(slurm_pilot_executor, "DEFAULT_MAP_TASKS", 2)
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        got = executor.map("cpu", square, range(10))

        assert got == [x * x for x in range(10)]
        submitted = [i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".task." in i]
        assert len(submitted) == 2

    def test_num_tasks_and_desc_are_keyword_only(self, executor):
        with pytest.raises(TypeError):
            executor.map("cpu", square, range(4), 2)
