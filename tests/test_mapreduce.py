"""Tests for `SlurmPilotExecutor.mapreduce`.

Slurm is mocked. The task queue is a real ds-service.
`mapreduce` blocks as soon as it submits,
so nothing can play the worker's part after the fact.
Every test that needs a result therefore runs a real worker in a thread,
the way `test_explore_space.py` does.

The worker-side function `_mapreduce_task` reads two variables
that `PilotWorker.__init__` sets,
`DS_SERVER_ADDRESS` and `PILOT_WORKER_ID`.
A test that runs a real worker therefore gets them from the worker.
That makes those tests a check on the worker as well.
A test that calls `_mapreduce_task` directly sets them itself,
through the `mapreduce_env` fixture.
"""

from __future__ import annotations

import json
import operator
import threading
from pathlib import Path

import pytest
import cloudpickle
from typeguard import TypeCheckError
from ds_service_client import DsServiceClient, TaskState

from slurm_workflows.slurm_pilot_executor import (
    MAPREDUCE_TOKEN_LEN,
    SlurmPilotExecutor,
    _mapreduce_task,
)
from slurm_workflows.slurm_pilot_worker import current_actor
from slurm_workflows.swtop import ALL_TASK_IDS

import support_actor

from worker_harness import make_worker, run_worker

# --------------------------------------------------------------------------
# What a call is made of
# --------------------------------------------------------------------------


def identity(x):
    return x


def add(acc, x):
    return acc + x


def scale(x, factor, offset=0):
    return x * factor + offset


def add_mod(acc, x, modulus=1000):
    """An associative `reduce_fn` that takes an extra keyword argument."""
    return (acc + x) % modulus


def wrap(x):
    """Map one item to a one-item list, so `add` concatenates."""
    return [x]


def extend_in_place(acc, x):
    """A `reduce_fn` that folds into its accumulator rather than replacing it."""
    acc.extend(x)
    return acc


def explode(x):
    raise ValueError(f"no good: {x}")


def count_hits(path, threshold):
    """The map function the how-to guide shows."""
    with open(path) as fobj:
        return sum(1 for line in fobj if float(line.split(",")[2]) > threshold)


# --------------------------------------------------------------------------
# Fixtures and drivers
# --------------------------------------------------------------------------


@pytest.fixture
def mapreduce_env(monkeypatch, ds_service_address):
    """What a worker puts in the environment, for a task driven without one."""
    monkeypatch.setenv("DS_SERVER_ADDRESS", ds_service_address)
    monkeypatch.setenv("PILOT_WORKER_ID", "test-worker.42.testhost.4242")


@pytest.fixture
def worker_thread(ds_service_address, tmp_path):
    """Run a real worker in a thread until it completes `expect_tasks`."""
    started: list[tuple] = []

    def start(
        expect_tasks: int,
        group: str = "cpu",
        name: str = "worker-0",
        actor_class_name: str = "",
    ):
        worker = make_worker(
            ds_service_address,
            tmp_path / name,
            group=group,
            name=name,
            actor_class_name=actor_class_name,
        )
        thread = threading.Thread(
            target=run_worker, args=(worker, expect_tasks), daemon=True
        )
        thread.start()
        started.append((worker, thread))

    yield start

    for worker, thread in started:
        thread.join(timeout=30)
        worker.close()


class RecordingClient:
    """Records the order of the calls a test asserts on."""

    def __init__(self, inner):
        self._inner = inner
        self.added_ids: list[str] = []

    def task_add(self, task_id, **kwargs):
        self.added_ids.append(task_id)
        return self._inner.task_add(task_id=task_id, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def item_ids(ds_client) -> list[str]:
    """Every mapreduce item task on the server, in id order."""
    return sorted(i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".item." in i)


# --------------------------------------------------------------------------
# results
# --------------------------------------------------------------------------


class TestResults:
    def test_it_sums_a_range(self, executor, pilot_jobs, worker_thread):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        total = executor.mapreduce(
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
        worker_thread(expect_tasks=3)

        got = executor.mapreduce(
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
        worker_thread(expect_tasks=2)

        total = executor.mapreduce(
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
        worker_thread(expect_tasks=1 + 8)

        one = executor.mapreduce(
            desc="one",
            queue="cpu",
            map_fn=identity,
            reduce_fn=add,
            iterable=range(20),
            init=0,
            num_tasks=1,
        )
        many = executor.mapreduce(
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
        worker_thread(expect_tasks=2)

        total = executor.mapreduce(
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
        worker_thread(expect_tasks=2)
        init: list[int] = []

        got = executor.mapreduce(
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
        """The example in `docs/how-to-guides/fold-results-across-workers.md`."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=4)

        paths = []
        for index in range(20):
            path = tmp_path / f"rows-{index}.csv"
            path.write_text("".join(f"a,b,{i}.0\n" for i in range(index)))
            paths.append(str(path))

        hits = executor.mapreduce(
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
        """Mapping to a one-item list and concatenating, as the guide shows."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        got = executor.mapreduce(
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
    def actor_group(self, executor):
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

        total = executor.mapreduce(
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
        """One actor per worker, not one per item, is the whole point."""
        support_actor.INSTANCES.clear()
        worker_thread(
            expect_tasks=1, group="act", actor_class_name="support_actor.MapActor"
        )

        executor.mapreduce(
            desc="scale",
            queue="act",
            map_fn="scale",
            reduce_fn=add,
            iterable=range(6),
            init=0,
            num_tasks=1,
        )

        actor = current_actor()
        assert isinstance(actor, support_actor.MapActor)
        assert actor.calls == 6

    def test_map_extra_args_reach_the_method(
        self, executor, actor_group, worker_thread
    ):
        worker_thread(
            expect_tasks=1, group="act", actor_class_name="support_actor.MapActor"
        )

        total = executor.mapreduce(
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
            executor.mapreduce(
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
            expect_tasks=2, group="act", actor_class_name="support_actor.MapActor"
        )

        total = executor.mapreduce(
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


class TestMapreduceTask:
    def test_concurrent_tasks_split_the_queue(
        self, ds_client, ds_service_address, mapreduce_env
    ):
        """Three tasks on one queue fold every item exactly once between them."""
        queue = "mr-direct"
        for index in range(60):
            ds_client.task_add(
                task_id=f"{queue}.item.{index}",
                queue=[queue],
                priority=float(-index),
                function=b"",
                input=cloudpickle.dumps(index),
            )

        partials: list[list[int]] = []
        lock = threading.Lock()

        def fold():
            got = _mapreduce_task(queue, wrap, add, [], (), {}, (), {})
            with lock:
                partials.append(got)

        threads = [threading.Thread(target=fold) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not any(t.is_alive() for t in threads)
        assert sorted(i for partial in partials for i in partial) == list(range(60))

    def test_it_marks_each_item_done(
        self, ds_client, ds_service_address, mapreduce_env
    ):
        queue = "mr-done"
        for index in range(5):
            ds_client.task_add(
                task_id=f"{queue}.item.{index}",
                queue=[queue],
                priority=float(-index),
                function=b"",
                input=cloudpickle.dumps(index),
            )

        assert _mapreduce_task(queue, identity, add, 0, (), {}, (), {}) == 10

        for index in range(5):
            task_id = f"{queue}.item.{index}"
            assert ds_client.task_get_status(task_id) == TaskState.Complete
            # The mapped value went home in the task's return value,
            # so the item task stores nothing of its own.
            assert ds_client.task_get_output(task_id) == b""

    def test_an_empty_queue_returns_init(self, ds_service_address, mapreduce_env):
        assert _mapreduce_task("mr-empty", identity, add, 7, (), {}, (), {}) == 7


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

        executor.mapreduce(
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
        assert len(tasks) == 2
        assert max(items) < min(tasks)

    def test_item_ids_do_not_collide_with_task_ids(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        executor.mapreduce(
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
        prefix = f"{executor.name}.mapreduce.0."
        assert all(i.startswith(prefix) for i in items)
        # `<name>.mapreduce.<index>.<token>.item.<i>`, and the token is hex.
        token = items[0][len(prefix) :].split(".")[0]
        assert len(token) == MAPREDUCE_TOKEN_LEN
        assert int(token, 16) >= 0

        # The submitted tasks kept the plain numbering, and nothing collided.
        assert executor.submit("cpu", identity, 1).task_id == f"{executor.name}.task.2"

    def test_two_calls_use_two_queues(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        for _ in range(2):
            executor.mapreduce(
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
        assert {q.split(".")[2] for q in queues} == {"0", "1"}

    def test_it_publishes_progress(
        self, executor, ds_client, pilot_jobs, worker_thread
    ):
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        executor.mapreduce(
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
        assert display["total"] == 3

    def test_it_names_its_tasks(self, executor, ds_client, pilot_jobs, worker_thread):
        """What `swtop` shows for a mapreduce task."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=2)

        executor.mapreduce(
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
            for i in range(2)
        }
        assert names == {
            f"{q}.task.{i}"
            for i in range(2)
            for q in {j.split(".item.")[0] for j in item_ids(ds_client)}
        }


# --------------------------------------------------------------------------
# edge cases and refusals
# --------------------------------------------------------------------------


class TestEdgeCases:
    def test_an_empty_iterable_returns_init(self, executor, ds_client):
        """No worker, no queue, no task: the call never waits."""
        assert (
            executor.mapreduce(
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
        assert executor.next_mapreduce_index == 0

    def test_an_empty_iterable_copies_init(self, executor):
        init: list[int] = []
        got = executor.mapreduce(
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
        """`num_tasks` is an upper bound: no task is submitted for no item."""
        pilot_jobs("cpu")
        worker_thread(expect_tasks=3)

        total = executor.mapreduce(
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
        assert len(submitted) == 3

    def test_num_tasks_below_one_raises(self, executor, ds_client):
        with pytest.raises(ValueError, match="num_tasks"):
            executor.mapreduce(
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
            executor.mapreduce(
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
            executor.mapreduce(
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
        self, ds_service_address, mapreduce_env
    ):
        """The worker-side half of the same check, for a group defined elsewhere."""
        with pytest.raises(RuntimeError, match="no actor"):
            _mapreduce_task("mr-no-actor", "scale", add, 0, (), {}, (), {})

    def test_a_queue_with_no_worker_raises_before_enqueueing(self, executor, ds_client):
        with pytest.raises(RuntimeError, match="no worker started"):
            executor.mapreduce(
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
            executor.mapreduce(
                desc="boom",
                queue="cpu",
                map_fn=explode,
                reduce_fn=add,
                iterable=range(4),
                init=0,
                num_tasks=1,
            )
