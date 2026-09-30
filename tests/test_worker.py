"""Tests for the worker."""

# How these tests run a real worker and stop it:
# see docs/how-to-run-tests.md, "Notes for future changes".

from __future__ import annotations

import json
import logging
import os
import sys
import signal
import threading
from pathlib import Path
from datetime import datetime
from typing import Any, Callable, Generator, NoReturn, cast

import pytest
import click
from ds_service_client import DsServiceClient, TaskState

import support_actor
from slurm_workflows import slurm_pilot_worker as worker_mod
from slurm_workflows.slurm_pilot_executor import RaiseOnError
from slurm_workflows.slurm_pilot_worker import current_actor, slurm_pilot_worker
from slurm_workflows.utils import RemoteExecutionError
from conftest import FakeGpu
from worker_harness import make_worker, poll_worker, run_until_restart, run_worker
from test_monitors import wait_for


@pytest.fixture(autouse=True)
def reset_actors() -> Generator[None]:
    support_actor.reset()
    yield
    support_actor.reset()


def square(x: int) -> int:
    return x * x


def boom() -> NoReturn:
    raise ValueError("task blew up")


def restart_own_group(value: int) -> int:
    """Ask the group of the worker running this task to restart, then return."""
    # The worker put the server address and its group in the environment.
    with DsServiceClient() as client:
        client.counter_get_next_value(
            f"{worker_mod.RESTART_GENERATION_PREFIX}{os.environ['PILOT_JOB_GROUP']}"
        )
    return value


def request_restart(client: DsServiceClient, group: str = "cpu") -> int:
    """What `SlurmPilotExecutor.restart_jobs` does to the server."""
    return client.counter_get_next_value(
        f"{worker_mod.RESTART_GENERATION_PREFIX}{group}"
    )


class TestTaskExecution:
    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs: Callable[..., None]) -> None:
        pilot_jobs("cpu", "gpu")

    def test_runs_a_task_and_posts_the_result(
        self, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", square, 7)

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == 49

    def test_idles_quietly_on_an_empty_queue(
        self, ds_service_address, tmp_path, caplog
    ):
        """An empty queue is the normal idle case, not an error."""
        worker = make_worker(ds_service_address, tmp_path)

        with caplog.at_level(logging.ERROR, logger="worker_process"):
            polls = poll_worker(worker, polls=3)

        worker.close()
        assert polls == 3, "the worker stopped polling an empty queue"
        # `NoTaskAvailable` left to the catch-all handler still polls,
        # but logs a traceback every time round.
        # Only the absence of that log tells the two apart.
        assert "Unexpected exception" not in caplog.text

    def test_runs_many_tasks_in_sequence(self, executor, ds_service_address, tmp_path):
        tasks = [executor.submit("cpu", square, i) for i in range(5)]

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=5)
        worker.close()

        executor.wait(tasks, desc="test")
        assert sorted(t.output for t in tasks) == [0, 1, 4, 9, 16]

    def test_runs_closures(self, executor, ds_service_address, tmp_path):
        offset = 100
        task = executor.submit("cpu", lambda x: x + offset, 5)

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == 105

    def test_passes_args_and_kwargs(self, executor, ds_service_address, tmp_path):
        def combine(a, b, sep="-"):
            return f"{a}{sep}{b}"

        task = executor.submit("cpu", combine, "x", "y", sep="+")

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == "x+y"

    def test_only_serves_its_own_group(
        self, executor, ds_service_address, ds_client, tmp_path
    ):
        cpu_task = executor.submit("cpu", square, 2)
        gpu_task = executor.submit("gpu", square, 3)

        worker = make_worker(ds_service_address, tmp_path, group="cpu")
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([cpu_task], desc="test")
        assert cpu_task.output == 4
        # The other group's task is untouched.
        assert ds_client.task_get_status(gpu_task.task_id) == TaskState.Ready


class TestRemoteErrors:
    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs: Callable[..., None]) -> None:
        pilot_jobs("cpu")

    def test_exception_is_captured_not_propagated(
        self, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", boom)

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)  # must not raise
        worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(task.output, RemoteExecutionError)
        assert task.output.error == "task blew up"
        assert task.output.error_id.startswith("ERROR_")

    def test_a_raising_task_is_marked_failed(
        self, executor, ds_client, ds_service_address, tmp_path
    ):
        """Failed, not Finished, so the tasks waiting on it fail too."""
        task = executor.submit("cpu", boom)
        child = executor.submit("cpu", square, 2, task_parents=[task])

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        assert ds_client.task_get_status(task.task_id) == TaskState.Failed
        assert ds_client.task_get_status(child.task_id) == TaskState.Failed

    def test_worker_survives_a_failing_task(
        self, executor, ds_service_address, tmp_path
    ):
        bad = executor.submit("cpu", boom)
        good = executor.submit("cpu", square, 4)

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=2)
        worker.close()

        executor.wait([bad, good], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(bad.output, RemoteExecutionError)
        assert good.output == 16

    def test_error_id_is_logged_with_the_traceback(
        self, executor, ds_service_address, tmp_path, caplog
    ):
        task = executor.submit("cpu", boom)

        with caplog.at_level(logging.ERROR, logger="worker_process"):
            worker = make_worker(ds_service_address, tmp_path)
            run_worker(worker, expect_tasks=1)
            worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        error_id = task.output.error_id
        assert error_id in caplog.text
        assert "ValueError: task blew up" in caplog.text

    def test_unserializable_result_is_reported_as_an_error(
        self, executor, ds_service_address, tmp_path
    ):
        # A generator cannot be pickled, so the result fails to serialize.
        # Serialization happens inside the try block,
        # so the handler catches it too.
        task = executor.submit("cpu", lambda: (_ for _ in range(3)))

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(task.output, RemoteExecutionError)


class TestActors:
    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs: Callable[..., None]) -> None:
        pilot_jobs("cpu")

    def test_actor_is_instantiated_once_at_startup(self, ds_service_address, tmp_path):
        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )

        assert len(support_actor.INSTANCES) == 1
        worker.close()

    def test_constructor_arguments_come_from_the_key_value_store(
        self, executor, ds_service_address, tmp_path
    ):
        executor.define_job_group(
            name="configured",
            sbatch_args=[],
            actor_class_name="support_actor.ConfiguredActor",
            actor_class_args=[1, "two"],
            actor_class_kwargs={"flag": True},
        )

        worker = make_worker(
            ds_service_address,
            tmp_path,
            group="configured",
            actor_class_name="support_actor.ConfiguredActor",
        )

        actor = worker.actor_instance
        assert actor is not None
        assert actor.args == (1, "two")
        assert actor.kwargs == {"flag": True}
        worker.close()

    def test_arguments_are_read_for_this_workers_group_only(
        self, executor, ds_service_address, tmp_path
    ):
        executor.define_job_group(
            name="configured",
            sbatch_args=[],
            actor_class_name="support_actor.ConfiguredActor",
            actor_class_args=[1, "two"],
        )

        # Same actor class, a different group: the keys are group-scoped,
        # so this worker builds its actor with nothing.
        worker = make_worker(
            ds_service_address,
            tmp_path,
            group="cpu",
            actor_class_name="support_actor.ConfiguredActor",
        )

        actor = worker.actor_instance
        assert actor is not None
        assert actor.args == ()
        assert actor.kwargs == {}
        worker.close()

    def test_a_configured_actor_runs_tasks(
        self, executor, ds_service_address, tmp_path
    ):
        executor.define_job_group(
            name="configured",
            sbatch_args=[],
            actor_class_name="support_actor.ConfiguredActor",
            actor_class_args=[1],
            actor_class_kwargs={"flag": True},
        )
        # `as_completed` refuses to wait on a queue with no pilot job,
        # and the autouse fixture only declared one for "cpu".
        executor.scale_jobs("configured", 1)

        task = executor.submit("configured", "config")

        worker = make_worker(
            ds_service_address,
            tmp_path,
            group="configured",
            actor_class_name="support_actor.ConfiguredActor",
        )
        run_worker(worker, 1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == ((1,), {"flag": True})

    def test_no_actor_by_default(self, ds_service_address, tmp_path):
        worker = make_worker(ds_service_address, tmp_path)

        assert worker.actor_instance is None
        worker.close()

    def test_method_names_dispatch_to_the_actor(
        self, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", "echo", "hello")

        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == "hello"

    def test_state_persists_across_tasks(self, executor, ds_service_address, tmp_path):
        tasks = [executor.submit("cpu", "bump", 1) for _ in range(4)]

        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        run_worker(worker, expect_tasks=4)
        worker.close()

        executor.wait(tasks, desc="test")
        # Same instance served all four, so the counter accumulated.
        assert sorted(t.output for t in tasks) == [1, 2, 3, 4]
        assert len(support_actor.INSTANCES) == 1

    def test_actor_exception_is_captured(self, executor, ds_service_address, tmp_path):
        task = executor.submit("cpu", "boom")

        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(task.output, RemoteExecutionError)
        assert task.output.error == "actor failure"

    def test_a_callable_runs_as_itself_on_an_actor_worker(
        self, executor, ds_service_address, tmp_path
    ):
        """The payload decides: a string is a method name, a callable is not."""
        task = executor.submit("cpu", square, 7)

        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], desc="test")
        assert task.output == 49

    def test_a_method_name_without_an_actor_is_captured(
        self, executor, ds_service_address, tmp_path
    ):
        task = executor.submit("cpu", "echo", "hello")

        worker = make_worker(ds_service_address, tmp_path)
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(task.output, RemoteExecutionError)
        assert "no actor" in task.output.error

    def test_the_actor_is_reachable_from_a_task(self, ds_service_address, tmp_path):
        """What a task that dispatches a method name of its own reads."""
        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )

        assert current_actor() is worker.actor_instance
        worker.close()
        assert current_actor() is None

    def test_unknown_method_is_captured(self, executor, ds_service_address, tmp_path):
        task = executor.submit("cpu", "no_such_method")

        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        run_worker(worker, expect_tasks=1)
        worker.close()

        executor.wait([task], raise_on_error=RaiseOnError.RAISE_NEVER, desc="test")
        assert isinstance(task.output, RemoteExecutionError)

    def test_close_calls_actor_close(self, ds_service_address, tmp_path):
        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.CounterActor"
        )
        actor = support_actor.INSTANCES[0]

        worker.close()

        assert actor.closed is True
        assert worker.actor_instance is None

    def test_close_tolerates_actor_without_close(self, ds_service_address, tmp_path):
        worker = make_worker(
            ds_service_address, tmp_path, actor_class_name="support_actor.NoCloseActor"
        )

        worker.close()  # must not raise

    def test_unimportable_actor_fails_at_startup(self, ds_service_address, tmp_path):
        with pytest.raises(ModuleNotFoundError):
            make_worker(
                ds_service_address, tmp_path, actor_class_name="no_such_module.Actor"
            )

    def test_missing_actor_class_fails_at_startup(self, ds_service_address, tmp_path):
        with pytest.raises(AttributeError):
            make_worker(
                ds_service_address, tmp_path, actor_class_name="support_actor.Missing"
            )


class TestWorkerIdentity:
    def test_worker_id_encodes_placement(self, ds_service_address, tmp_path):
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        assert worker.worker_id == "w-1.42.testhost.4242"
        worker.close()

    def test_startup_puts_the_identity_in_the_environment(
        self, ds_service_address, tmp_path
    ):
        """What a task reads to reach the server and to name itself on it."""
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        assert os.environ["PILOT_JOB_NAME"] == "w-1"
        assert os.environ["PILOT_JOB_GROUP"] == "cpu"
        assert os.environ["PILOT_WORKER_ID"] == worker.worker_id
        assert os.environ["DS_SERVER_ADDRESS"] == ds_service_address
        worker.close()

    def test_the_environment_is_set_before_the_actor_is_built(
        self, ds_service_address, tmp_path
    ):
        """An actor constructor can open a client of its own."""
        worker = make_worker(
            ds_service_address,
            tmp_path,
            group="cpu",
            name="w-1",
            actor_class_name="support_actor.EnvironmentActor",
        )

        actor = worker.actor_instance
        assert actor is not None
        assert actor.server_address == ds_service_address
        assert actor.worker_id == worker.worker_id
        worker.close()

    def test_startup_publishes_the_workers_identity(
        self, ds_service_address, ds_client, tmp_path
    ):
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        published = json.loads(ds_client.map_get(f"worker_info:{worker.worker_id}"))

        start_time = published.pop("start_time")
        assert published == {
            "group": "cpu",
            "name": "w-1",
            "slurm_job_id": 42,
            "hostname": "testhost",
            "pid": 4242,
            "restart_generation": 0,
        }
        assert datetime.fromisoformat(start_time).tzinfo is not None
        worker.close()

    def test_the_identity_carries_the_restart_generation(
        self, ds_service_address, ds_client, tmp_path
    ):
        """How `restart_jobs` tells a worker that restarted from one that did not."""
        request_restart(ds_client)
        request_restart(ds_client)

        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        published = json.loads(ds_client.map_get(f"worker_info:{worker.worker_id}"))
        assert worker.restart_generation == 2
        assert published["restart_generation"] == 2
        worker.close()

    def test_the_identity_is_one_key_not_one_per_field(
        self, ds_service_address, ds_client, tmp_path
    ):
        """A reader between two writes must not see a half-described worker."""
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        assert ds_client.map_search_key(f"^worker_.*:{worker.worker_id}$") == [
            f"worker_info:{worker.worker_id}"
        ]
        worker.close()

    def test_two_workers_publish_separately(
        self, ds_service_address, ds_client, tmp_path
    ):
        first = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")
        second = make_worker(ds_service_address, tmp_path, group="gpu", name="w-2")

        for worker, group in [(first, "cpu"), (second, "gpu")]:
            published = json.loads(ds_client.map_get(f"worker_info:{worker.worker_id}"))
            assert published["group"] == group
        first.close()
        second.close()

    def test_identity_is_published_before_the_actor_is_built(
        self, ds_service_address, ds_client, tmp_path
    ):
        """A worker that dies constructing its actor already published where it was."""
        with pytest.raises(AttributeError):
            make_worker(
                ds_service_address,
                tmp_path,
                group="cpu",
                name="w-1",
                actor_class_name="support_actor.Missing",
            )

        wid = "w-1.42.testhost.4242"
        published = json.loads(ds_client.map_get(f"worker_info:{wid}"))
        assert published["hostname"] == "testhost"

    def test_nothing_says_a_running_worker_exited(
        self, ds_service_address, ds_client, tmp_path
    ):
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        assert ds_client.map_search_key("^worker_exit:") == []
        worker.close()

    def test_close_publishes_the_exit(self, ds_service_address, ds_client, tmp_path):
        worker = make_worker(ds_service_address, tmp_path, group="cpu", name="w-1")

        worker.close()

        published = json.loads(ds_client.map_get(f"worker_exit:{worker.worker_id}"))
        assert datetime.fromisoformat(published["exit_time"]).tzinfo is not None

    def test_a_worker_that_dies_building_its_actor_publishes_its_exit(
        self, ds_service_address, ds_client, tmp_path
    ):
        with pytest.raises(AttributeError):
            make_worker(
                ds_service_address,
                tmp_path,
                group="cpu",
                name="w-1",
                actor_class_name="support_actor.Missing",
            )

        assert ds_client.map_search_key("^worker_exit:") == [
            "worker_exit:w-1.42.testhost.4242"
        ]


class TestMonitors:
    """Within each job, one worker per node samples the node, the job and its GPUs."""

    def test_the_first_worker_takes_on_both(self, ds_service_address, tmp_path):
        worker = make_worker(ds_service_address, tmp_path)

        assert [m.subject for m in worker.monitors] == ["testhost", "42:testhost"]
        worker.close()

    def test_a_peer_on_the_same_node_and_job_takes_on_neither(
        self, ds_service_address, tmp_path
    ):
        first = make_worker(ds_service_address, tmp_path, name="w-1")
        second = make_worker(ds_service_address, tmp_path, name="w-2")

        assert second.monitors == []
        first.close()
        second.close()

    def test_a_worker_on_another_node_takes_on_that_node(
        self, ds_service_address, tmp_path
    ):
        first = make_worker(ds_service_address, tmp_path)
        second = make_worker(ds_service_address, tmp_path, hostname="othernode")

        # Same job, but each node samples its own part of it.
        assert [m.subject for m in second.monitors] == ["othernode", "42:othernode"]
        first.close()
        second.close()

    def test_a_worker_in_another_job_takes_on_its_node_and_its_job(
        self, ds_service_address, tmp_path
    ):
        """Two jobs that share a node each sample it."""
        first = make_worker(ds_service_address, tmp_path)
        second = make_worker(ds_service_address, tmp_path, slurm_job_id=99)

        assert [m.subject for m in second.monitors] == ["testhost", "99:testhost"]
        first.close()
        second.close()

    def test_a_later_job_on_a_reused_node_samples_it_again(
        self, ds_service_address, ds_client, tmp_path
    ):
        """The node's series must not stay stale once its first job is gone."""
        first = make_worker(ds_service_address, tmp_path, name="w-1")
        first.close()
        # A load average cannot be negative, so a value >= 0 is a fresh sample.
        ds_client.time_series_append(
            "host_load_average:testhost", -1.0, "2000-01-01T00:00:00+00:00"
        )

        second = make_worker(ds_service_address, tmp_path, slurm_job_id=99)

        assert "testhost" in [m.subject for m in second.monitors]
        assert wait_for(
            lambda: ds_client.time_series_get("host_load_average:testhost")[-1].value
            >= 0.0
        )
        second.close()

    def test_the_election_is_a_counter_per_subject(
        self, ds_service_address, ds_client, tmp_path
    ):
        first = make_worker(ds_service_address, tmp_path, name="w-1")
        second = make_worker(ds_service_address, tmp_path, name="w-2")

        # The last part is the restart generation, 0 before any restart.
        assert ds_client.counter_get_current_value("host_monitor:testhost:42:0") == 2
        first.close()
        second.close()

    def test_a_restarted_worker_samples_again(
        self, ds_service_address, ds_client, tmp_path
    ):
        """The same job and node as before, but a new election counter."""
        first = make_worker(ds_service_address, tmp_path, name="w-1")
        first.close()
        request_restart(ds_client)

        second = make_worker(ds_service_address, tmp_path, name="w-1")
        third = make_worker(ds_service_address, tmp_path, name="w-2")

        assert [m.subject for m in second.monitors] == ["testhost", "42:testhost"]
        # A peer at the same generation still leaves the node to the first.
        assert third.monitors == []
        second.close()
        third.close()

    def test_a_first_reading_is_published_at_startup(
        self, ds_service_address, ds_client, tmp_path
    ):
        """The tables in swtop must not be empty until the first interval."""
        worker = make_worker(ds_service_address, tmp_path)

        assert wait_for(
            lambda: bool(ds_client.time_series_get("host_free_memory:testhost"))
        )
        assert wait_for(
            lambda: bool(ds_client.time_series_get("slurm_job_memory:42:testhost"))
        )
        worker.close()

    def test_on_a_gpu_node_it_samples_the_gpus_too(
        self, ds_service_address, ds_client, tmp_path, fake_nvml
    ):
        fake_nvml.gpus = [FakeGpu()]

        worker = make_worker(ds_service_address, tmp_path)

        assert [m.name for m in worker.monitors][-1] == "gpu-monitor:42:testhost"
        assert wait_for(
            lambda: bool(
                ds_client.time_series_get("slurm_job_gpu_utilization:42:testhost:0")
            )
        )
        worker.close()

    def test_only_the_elected_worker_samples_the_gpus(
        self, ds_service_address, tmp_path, fake_nvml
    ):
        fake_nvml.gpus = [FakeGpu()]

        first = make_worker(ds_service_address, tmp_path, name="w-1")
        second = make_worker(ds_service_address, tmp_path, name="w-2")

        assert len(first.monitors) == 3
        assert second.monitors == []
        first.close()
        second.close()

    def test_a_failed_actor_leaves_none_of_them_running(
        self, ds_service_address, tmp_path
    ):
        """The worker never calls close() on a constructor that raised."""
        before = {t for t in threading.enumerate()}

        with pytest.raises(AttributeError):
            make_worker(
                ds_service_address, tmp_path, actor_class_name="support_actor.Missing"
            )

        leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
        assert leaked == []

    def test_close_stops_them(self, ds_service_address, tmp_path):
        worker = make_worker(ds_service_address, tmp_path)
        monitors = list(worker.monitors)

        worker.close()

        assert monitors, "this worker was supposed to have started some"
        assert not any(m.is_alive() for m in monitors)
        assert worker.monitors == []


# Both doubles below forward every call they do not override to the real client,
# so a test casts them to `DsServiceClient` to put them on `worker.client`.
class _RestartingClient:
    """Wraps a real client, and requests a restart on the Nth `task_get` call."""

    def __init__(self, inner: DsServiceClient, group: str, at_poll: int) -> None:
        self._inner = inner
        self._group = group
        self._at_poll = at_poll
        self.polls = 0

    def task_get(self, worker_id: str, queue: str | list[str]):
        self.polls += 1
        if self.polls == self._at_poll:
            # The request comes before this call reaches the server.
            request_restart(self._inner, self._group)
        return self._inner.task_get(worker_id, queue)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _FailingCounterClient:
    """Wraps a real client, and fails the counter reads that `fail` picks."""

    def __init__(self, inner: DsServiceClient, fail: Callable[[int], bool]) -> None:
        self._inner = inner
        self._fail = fail
        self.reads = 0
        # Each read and each fetch, in order.
        self.events: list[str] = []

    def counter_get_current_value(self, key: str) -> int:
        self.reads += 1
        # `fail` gets the number of the read, counting from 1.
        if self._fail(self.reads):
            self.events.append("read-failed")
            raise TimeoutError("server unreachable")
        self.events.append("read")
        return self._inner.counter_get_current_value(key)

    def task_get(self, worker_id: str, queue: str | list[str]):
        self.events.append("fetch")
        return self._inner.task_get(worker_id, queue)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class TestRestart:
    """A worker returns from `main()` once its group's restart counter moves.

    The worker checks between tasks, never during one.
    """

    @pytest.fixture(autouse=True)
    def _pilot_jobs(self, pilot_jobs: Callable[..., None]) -> None:
        pilot_jobs("cpu", "gpu")

    @pytest.fixture
    def check_every_time(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Read the counter at every turn of the loop, not once per interval.

        As a result, the count of fetches before `main()` returns is exact.
        """
        monkeypatch.setattr(worker_mod, "RESTART_CHECK_INTERVAL_S", 0.0)

    @pytest.fixture
    def check_rarely(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Leave only the checks that the interval does not limit.

        These checks are the first check and the one right after a task.
        """
        monkeypatch.setattr(worker_mod, "RESTART_CHECK_INTERVAL_S", 60.0)

    def test_an_idle_worker_returns_after_a_restart_request(
        self, check_every_time, ds_service_address, tmp_path, time_limit
    ):
        worker = make_worker(ds_service_address, tmp_path)
        worker.client = cast(
            DsServiceClient, _RestartingClient(worker.client, "cpu", at_poll=3)
        )

        with time_limit(10, "the worker never returned for a restart"):
            polls = run_until_restart(worker, max_polls=10)

        worker.close()
        # The third fetch still went out, since the request came right before it.
        assert polls == 3

    def test_an_idle_worker_sees_the_request_within_the_interval(
        self, monkeypatch, ds_service_address, tmp_path, time_limit
    ):
        """The rate limit delays the return of an idle worker, but does not drop it."""
        monkeypatch.setattr(worker_mod, "RESTART_CHECK_INTERVAL_S", 0.3)
        worker = make_worker(ds_service_address, tmp_path)
        worker.client = cast(
            DsServiceClient, _RestartingClient(worker.client, "cpu", at_poll=2)
        )

        with time_limit(10, "the worker never returned for a restart"):
            polls = run_until_restart(worker, max_polls=50)

        worker.close()
        assert polls >= 2

    def test_a_request_older_than_the_worker_is_not_for_it(
        self, check_every_time, ds_service_address, ds_client, tmp_path
    ):
        """A worker that started at generation N already runs the new code."""
        request_restart(ds_client)
        request_restart(ds_client)

        worker = make_worker(ds_service_address, tmp_path)
        polls = poll_worker(worker, polls=3)

        worker.close()
        assert worker.restart_generation == 2
        assert polls == 3, "the worker restarted for a request made before it started"

    def test_the_first_check_comes_before_the_first_claim(
        self, check_rarely, executor, ds_service_address, ds_client, tmp_path
    ):
        """A worker older than the request must not run a task on the old code."""
        worker = make_worker(ds_service_address, tmp_path)
        task = executor.submit("cpu", square, 3)
        request_restart(ds_client)

        polls = run_until_restart(worker, max_polls=5)

        worker.close()
        assert polls == 0
        assert ds_client.task_get_status(task.task_id) == TaskState.Ready

    def test_a_running_task_completes_before_the_restart(
        self, check_rarely, executor, ds_service_address, ds_client, tmp_path
    ):
        """The task itself requests the restart, so the request lands mid-task."""
        first = executor.submit("cpu", restart_own_group, 7)
        worker = make_worker(ds_service_address, tmp_path)

        # One fetch claims the task,
        # and `main()` returns at the check right after the task.
        polls = run_until_restart(worker, max_polls=5)
        second = executor.submit("cpu", square, 3)

        worker.close()
        assert polls == 1
        assert ds_client.task_get_status(first.task_id) == TaskState.Finished
        executor.wait([first], desc="test")
        assert first.output == 7
        # Nothing claimed a task after the worker saw the restart.
        assert ds_client.task_get_status(second.task_id) == TaskState.Ready

    def test_a_failed_task_also_counts_as_a_finished_one(
        self, check_rarely, executor, ds_service_address, ds_client, tmp_path
    ):
        """The check right after a task does not depend on how the task ended."""
        task = executor.submit("cpu", boom)
        worker = make_worker(ds_service_address, tmp_path)
        worker.client = cast(
            DsServiceClient, _RestartingClient(worker.client, "cpu", at_poll=1)
        )

        polls = run_until_restart(worker, max_polls=5)

        worker.close()
        assert polls == 1
        assert ds_client.task_get_status(task.task_id) == TaskState.Failed

    def test_a_restart_of_one_group_leaves_another_alone(
        self, check_every_time, ds_service_address, ds_client, tmp_path
    ):
        worker = make_worker(ds_service_address, tmp_path, group="gpu")
        request_restart(ds_client, "cpu")

        polls = poll_worker(worker, polls=3)

        worker.close()
        assert polls == 3, "the worker restarted for another group's request"

    def test_a_failed_counter_read_does_not_end_the_worker(
        self, check_every_time, ds_service_address, tmp_path, caplog
    ):
        """A server problem is not a restart request."""
        worker = make_worker(ds_service_address, tmp_path)
        # The first read succeeds, and every later one fails.
        worker.client = cast(
            DsServiceClient,
            _FailingCounterClient(worker.client, fail=lambda n: n > 1),
        )

        with caplog.at_level(logging.ERROR, logger="worker_process"):
            polls = poll_worker(worker, polls=3)

        worker.close()
        assert polls == 3
        assert "Failed to read the restart counter" in caplog.text

    def test_no_claim_before_a_read_of_the_counter_succeeds(
        self, check_rarely, ds_service_address, tmp_path, time_limit
    ):
        """A failed first read leaves the worker idle, not on the old code.

        A claim then could run a task after `restart_jobs` returned.
        """
        worker = make_worker(ds_service_address, tmp_path)
        client = _FailingCounterClient(worker.client, fail=lambda n: n <= 3)
        worker.client = cast(DsServiceClient, client)

        with time_limit(10, "the worker never read the counter again"):
            polls = poll_worker(worker, polls=2)

        worker.close()
        assert polls == 2
        # The rate limit did not apply to the reads that failed.
        assert client.events[:5] == [
            "read-failed",
            "read-failed",
            "read-failed",
            "read",
            "fetch",
        ]


class TestCli:
    """The console entry point.

    The cases cover its options, `sys.path`, SIGTERM, the pilot job events,
    and leaving the output streams alone.
    """

    @pytest.fixture(autouse=True)
    def _restore_process_state(self) -> Generator[None]:
        """Restore `sys.path`, the SIGTERM handler and the environment."""
        # The command prepends to `sys.path` and installs a SIGTERM handler,
        # and undoes neither, because in production the process is the worker.
        # Nothing else restores those two.
        # The autouse fixture in `conftest.py` already restores the environment,
        # so restoring it here as well is only belt and braces.
        env = dict(os.environ)
        path = list(sys.path)
        sigterm = signal.getsignal(signal.SIGTERM)
        yield
        os.environ.clear()
        os.environ.update(env)
        sys.path[:] = path
        signal.signal(signal.SIGTERM, sigterm)

    @pytest.fixture
    def captured(self, monkeypatch):
        """Replace the worker so `main()` returns at once, as it does for a restart."""
        seen = {}

        class FakeWorker:
            def __init__(self, **kwargs: object) -> None:
                seen["kwargs"] = kwargs

            def main(self) -> None:
                seen["sys_path_head"] = list(sys.path[:2])
                seen["streams"] = (sys.stdout, sys.stderr)

            def close(self) -> None:
                seen["closed"] = True

        monkeypatch.setattr(worker_mod, "PilotWorker", FakeWorker)
        return seen

    def invoke(self, tmp_path: Path, **overrides: str) -> int:
        """Run the CLI and return its exit code."""
        args = {
            "--group": "cpu",
            "--name": "worker-0",
            "--actor-class-name": "",
            "--server-address": "127.0.0.1:5051",
            "--work-dir": str(tmp_path),
            "--python-paths-json": '["/extra/path"]',
        }
        args.update(overrides)
        argv = [item for pair in args.items() for item in pair]

        # Not click's CliRunner: it swaps the process streams for buffers,
        # and these tests assert on what the command does to those streams.
        try:
            slurm_pilot_worker.main(
                args=argv, prog_name="slurm-pilot-worker", standalone_mode=False
            )
            return 0
        except SystemExit as exc:
            return exc.code if isinstance(exc.code, int) else 1
        except click.UsageError as exc:
            return exc.exit_code

    def test_runs_and_closes_the_worker(self, captured, tmp_path):
        """`main()` returns only for a restart, so the command asks for one."""
        exit_code = self.invoke(tmp_path)

        assert exit_code == worker_mod.RESTART_EXIT_CODE
        assert captured["closed"] is True

    def test_prepends_python_paths(self, captured, tmp_path):
        self.invoke(tmp_path)

        assert "/extra/path" in captured["sys_path_head"]

    def test_leaves_the_process_streams_alone(self, captured, tmp_path):
        """A redirect empties the file Slurm writes.

        See the logging comment in `slurm_pilot_worker.slurm_pilot_worker`.
        """
        before = (sys.stdout, sys.stderr)

        self.invoke(tmp_path)

        assert captured["streams"] == before

    def test_sigterm_still_closes_the_worker(self, captured, monkeypatch, tmp_path):
        """Slurm ends a job with SIGTERM, and the worker must still say it exited."""

        def terminated(self) -> None:
            os.kill(os.getpid(), signal.SIGTERM)

        # captured has already swapped in FakeWorker, so this patches its main.
        monkeypatch.setattr(worker_mod.PilotWorker, "main", terminated)

        exit_code = self.invoke(tmp_path)

        assert exit_code == 128 + signal.SIGTERM
        assert captured["closed"] is True

    @pytest.mark.parametrize("event", ["start", "exit"])
    def test_a_pilot_job_event_is_published_and_starts_no_worker(
        self, captured, ds_service_address, ds_client, tmp_path, event
    ):
        exit_code = self.invoke(
            tmp_path,
            **{
                "--server-address": ds_service_address,
                "--name": "testex.job.cpu.0",
                "--pilot-job-event": event,
            },
        )

        assert exit_code == 0
        assert "kwargs" not in captured
        published = json.loads(ds_client.map_get(f"pilot_job_{event}:testex.job.cpu.0"))
        assert datetime.fromisoformat(published[f"{event}_time"]).tzinfo is not None

    def test_rejects_missing_work_dir(self, captured, tmp_path):
        exit_code = self.invoke(tmp_path, **{"--work-dir": str(tmp_path / "nope")})

        assert exit_code != 0
