"""The worker: the process that claims and runs tasks on a compute node.

The generated worker script starts it inside a pilot job.
User code never constructs it.
"""

import os
import sys
import json
import time
import pickle
import socket
import logging
import importlib
from pathlib import Path
from typing import Any

import click
import cloudpickle
from ds_service_client import DsServiceClient, NoTaskAvailable

from .utils import gen_error_id, RemoteExecutionError, LOG_FORMAT, LOG_LEVEL
from .monitors import (
    DEFAULT_MONITOR_INTERVAL_S,
    Monitor,
    start_host_monitor,
    start_slurm_job_monitor,
)

# How long a worker sleeps before it asks again,
# after an empty queue or a failed request.
NEXT_TASK_RETRY_TIME_S: float = 0.1

# One JSON key per worker, keyed on its worker id.
# `swtop` reads it.
# `docs/reference/what-a-run-publishes.md` lists the fields.
WORKER_INFO_PREFIX = "worker_info:"

# The actor of the worker running in this process, or None.
# A task reads it through `current_actor()`,
# which is how a task dispatches a method name of its own.
_CURRENT_ACTOR: Any | None = None


def current_actor() -> Any | None:
    """The actor of the worker in this process, or None.

    A worker is one process and builds one actor,
    so a task that needs the actor reads it here.
    """
    return _CURRENT_ACTOR


def _set_current_actor(actor: Any | None) -> None:
    """Record the actor this process runs tasks against."""
    global _CURRENT_ACTOR
    _CURRENT_ACTOR = actor


class PilotWorker:
    """One worker that claims tasks from its job group's queue and runs them.

    The worker runs on a compute node inside a pilot job.
    The generated worker script starts it.
    User code never constructs it.
    """

    def __init__(
        self,
        group: str,
        name: str,
        actor_class_name: str,
        server_address: str,
        work_dir: Path,
        slurm_job_id: int,
        hostname: str,
        pid: int,
        monitor_interval: float = DEFAULT_MONITOR_INTERVAL_S,
    ) -> None:
        """Register this worker on the server and build its actor.

        Puts this worker's identity in the environment first,
        so the actor and every task it runs can read it.
        Publishes the worker's identity before it builds the actor.
        Whatever the actor's constructor raises propagates.
        Before that, this worker closes its own monitors and client.
        """
        self.group = group
        self.name = name
        self.server_address = server_address
        self.work_dir = work_dir

        # `<job-name>.<job-id>.<hostname>.<pid>`.
        # The job name carries the job group.
        self.worker_id = "%s.%s.%s.%s" % (name, slurm_job_id, hostname, pid)
        self.logger = logging.getLogger("worker_process")

        # Before the client and the actor, so a task this worker runs
        # can reach the server and name itself on it.
        self._publish_environment()

        self.client = DsServiceClient(self.server_address)

        # Before the actor is built, so a worker that dies building one
        # has still said where it died.
        self._publish_identity(slurm_job_id, hostname, pid)

        self.monitors: list[Monitor] = []
        self._start_monitors(hostname, slurm_job_id, monitor_interval)

        self.actor_instance: Any | None
        try:
            self.actor_instance = self._build_actor(actor_class_name)
        except Exception:
            # Nothing calls `close()` on a worker whose constructor raised,
            # so it stops what it started before it re-raises.
            self._stop_monitors()
            self.client.close()
            raise

        # For a task that dispatches a method name of its own,
        # such as the one `mapreduce` submits.
        _set_current_actor(self.actor_instance)

    def _build_actor(self, actor_class_name: str) -> Any | None:
        """Import and construct this job group's actor, if it has one."""
        if actor_class_name == "":
            return None

        class_name_parts = actor_class_name.split(".")
        module_name = ".".join(class_name_parts[:-1])
        class_name = class_name_parts[-1]

        module = importlib.import_module(module_name)
        klass = getattr(module, class_name)

        args = self._get_actor_ctor_arg(f"actor_class_args:{self.group}", [])
        kwargs = self._get_actor_ctor_arg(f"actor_class_kwargs:{self.group}", {})
        return klass(*args, **kwargs)

    def _publish_environment(self) -> None:
        """Put this worker's identity in the environment, for its tasks."""
        os.environ["PILOT_JOB_NAME"] = self.name
        os.environ["PILOT_JOB_GROUP"] = self.group
        os.environ["PILOT_WORKER_ID"] = self.worker_id
        os.environ["DS_SERVER_ADDRESS"] = self.server_address

    def _publish_identity(self, slurm_job_id: int, hostname: str, pid: int) -> None:
        """Record who this worker is in the map, as one JSON key."""
        identity = {
            "group": self.group,
            "name": self.name,
            "slurm_job_id": slurm_job_id,
            "hostname": hostname,
            "pid": pid,
        }
        self.client.map_set(
            f"{WORKER_INFO_PREFIX}{self.worker_id}",
            json.dumps(identity).encode("utf-8"),
        )

    def _start_monitors(
        self, hostname: str, slurm_job_id: int, interval: float
    ) -> None:
        """Monitor this node and this job, if no other worker already does."""
        # The counter hands out distinct values,
        # so exactly one worker sees 1 and takes the subject.
        if self.client.counter_get_next_value(f"host_monitor:{hostname}") == 1:
            self.logger.info("Monitoring host %s", hostname)
            self.monitors.append(
                start_host_monitor(self.client, hostname, interval, self.logger)
            )

        counter = f"slurm_job_monitor:{slurm_job_id}"
        if self.client.counter_get_next_value(counter) == 1:
            self.logger.info("Monitoring slurm job %s", slurm_job_id)
            self.monitors.append(
                start_slurm_job_monitor(
                    self.client, slurm_job_id, interval, self.logger
                )
            )

    def _get_actor_ctor_arg(self, key: str, default: Any) -> Any:
        """Read one cloudpickled constructor argument from the map."""
        try:
            value = self.client.map_get(key)
        except KeyError:
            # A missing key means the caller passed none.
            # See the developer notes.
            return default
        return cloudpickle.loads(value)

    def _resolve_method(self, name: str) -> Any:
        """Look one method name up on this worker's actor."""
        if self.actor_instance is None:
            raise RuntimeError(
                f"Task names the method {name!r}, "
                f"but job group {self.group!r} has no actor"
            )
        return getattr(self.actor_instance, name)

    def _stop_monitors(self) -> None:
        """Stop whatever monitoring this worker still runs."""
        for monitor in self.monitors:
            monitor.stop()
        # Cleared, so a second call does nothing.
        self.monitors.clear()

    def close(self) -> None:
        """Stop the monitors, close the connection, and close the actor.

        Calls the actor's own `close()` if it has one.
        """
        # Before the client, whose channel they use.
        self._stop_monitors()

        self.client.close()
        if self.actor_instance is not None:
            # Only this worker's own actor, since a test can build two
            # in one process and the second one is still running.
            if current_actor() is self.actor_instance:
                _set_current_actor(None)
            if hasattr(self.actor_instance, "close"):
                self.actor_instance.close()
            self.actor_instance = None

    def main(self) -> None:
        """Pull tasks from the group's queue and run them, forever.

        Never returns of its own accord:
        a worker lives until its Slurm job ends.
        The worker catches every `Exception` a task raises,
        logs it under a generated `error_id`,
        and returns it to the caller as a `RemoteExecutionError`
        on a task it marks Failed.
        As a result, one bad task cannot end the worker.
        """
        self.logger.info("Starting worker: %s" % self.worker_id)

        while True:
            try:
                try:
                    task = self.client.task_get(self.worker_id, self.group)
                except NoTaskAvailable:
                    # An idle queue: sleep and ask again.
                    # A TimeoutError is a server problem,
                    # and the outer `except` catches it.
                    time.sleep(NEXT_TASK_RETRY_TIME_S)
                    continue

                try:
                    self.logger.info(
                        "task_id=%s: Deserializing function and inputs ...",
                        task.task_id,
                    )
                    function = cloudpickle.loads(task.function)
                    if isinstance(function, str):
                        function = self._resolve_method(function)
                    args, kwargs = cloudpickle.loads(task.input)

                    self.logger.info("task_id=%s: Executing ...", task.task_id)
                    retval = function(*args, **kwargs)

                    self.logger.info("task_id=%s: Serializng output ...", task.task_id)
                    output = cloudpickle.dumps(retval, protocol=pickle.HIGHEST_PROTOCOL)

                    self.client.task_done(task.task_id, self.worker_id, output)
                except Exception as e:
                    eid = gen_error_id()
                    self.logger.exception(
                        "Error executing %s: %s: %s", task.task_id, eid, e
                    )

                    retval = RemoteExecutionError(error=str(e), error_id=eid)
                    output = cloudpickle.dumps(retval, protocol=pickle.HIGHEST_PROTOCOL)
                    # Failed, not Finished, so every task waiting on this one fails too.
                    self.client.task_done(
                        task.task_id, self.worker_id, output, failed=True
                    )
            except Exception:
                self.logger.exception("Unexpected exception")

                # A refused connection returns at once,
                # so without this sleep the loop spins on a core.
                time.sleep(NEXT_TASK_RETRY_TIME_S)


@click.command()
@click.option("--group", type=str, required=True, help="Job group name.")
@click.option("--name", type=str, required=True, help="Pilot job name.")
@click.option(
    "--actor-class-name",
    type=str,
    required=True,
    help="Fully qualified class name of the actor, or the empty string.",
)
@click.option(
    "--server-address", type=str, required=True, help="ds-service server address."
)
@click.option(
    "--work-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Work directory.",
)
@click.option(
    "--python-paths-json",
    type=str,
    required=True,
    help="JSON encoded Python paths.",
)
def slurm_pilot_worker(
    group: str,
    name: str,
    actor_class_name: str,
    server_address: str,
    work_dir: Path,
    python_paths_json: str,
) -> None:
    """Start a worker."""
    # Outside a Slurm job, the id is -1.
    slurm_job_id = int(os.environ.get("SLURM_JOB_ID", -1))
    hostname = socket.gethostname()
    pid = os.getpid()

    logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

    python_paths: list[str] = json.loads(python_paths_json)
    for path in python_paths:
        sys.path.insert(0, path)

    worker = PilotWorker(
        group=group,
        name=name,
        actor_class_name=actor_class_name,
        server_address=server_address,
        work_dir=work_dir,
        slurm_job_id=slurm_job_id,
        hostname=hostname,
        pid=pid,
    )

    try:
        worker.main()
    finally:
        worker.close()
