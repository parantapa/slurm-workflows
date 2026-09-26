"""The worker: the process that claims and runs tasks on a compute node."""

import os
import sys
import json
import time
import pickle
import signal
import socket
import logging
import importlib
from pathlib import Path
from types import FrameType
from datetime import datetime
from typing import Any, Literal

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

# One JSON key per worker that exited, keyed on its worker id,
# and one per pilot job that started and that exited, keyed on its job name.
# Each is written once, so `swtop` reads it once.
# `docs/reference/what-a-run-publishes.md` lists the fields.
WORKER_EXIT_PREFIX = "worker_exit:"
PILOT_JOB_START_PREFIX = "pilot_job_start:"
PILOT_JOB_EXIT_PREFIX = "pilot_job_exit:"

# The actor of the worker running in this process, or None.
# A task reads it through `current_actor()`,
# which is how a task dispatches a method name of its own.
_CURRENT_ACTOR: Any | None = None


def current_actor() -> Any | None:
    """The actor of the worker in this process.

    None outside a worker, and in a worker whose job group has no actor.
    """
    return _CURRENT_ACTOR


def _set_current_actor(actor: Any | None) -> None:
    """Record the actor this process runs tasks against."""
    global _CURRENT_ACTOR
    _CURRENT_ACTOR = actor


def _timestamp() -> str:
    """Now, as an ISO 8601 timestamp with the local offset."""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def publish_pilot_job_event(
    client: DsServiceClient, name: str, event: Literal["start", "exit"]
) -> None:
    """Record when the pilot job `name` started or exited, as one JSON key."""
    if event == "start":
        key, field = f"{PILOT_JOB_START_PREFIX}{name}", "start_time"
    else:
        key, field = f"{PILOT_JOB_EXIT_PREFIX}{name}", "exit_time"
    client.map_set(key, json.dumps({field: _timestamp()}).encode("utf-8"))


def _exit_on_sigterm(signum: int, frame: FrameType | None) -> None:
    """Turn the SIGTERM Slurm sends into a `SystemExit`, so cleanup runs."""
    # The status a shell reports for a death by that signal.
    raise SystemExit(128 + signum)


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

        Puts this worker's identity in the environment,
        so the actor and every task it runs can read it.
        Publishes that identity on the server,
        and starts the monitors of this node,
        unless another worker of this job on this node has taken them.
        Whatever importing or constructing the actor raises propagates,
        after this worker publishes its exit
        and closes its own monitors and client.
        Otherwise `current_actor()` returns the new actor.
        """
        self.group = group
        self.name = name
        self.server_address = server_address
        self.work_dir = work_dir

        # `<job-name>.<job-id>.<hostname>.<pid>`.
        # The job name carries the job group.
        self.worker_id = "%s.%s.%s.%s" % (name, slurm_job_id, hostname, pid)
        self.logger = logging.getLogger("worker_process")
        self._exit_published = False

        # Before the actor is built, so its constructor and every task
        # can reach the server and name themselves on it.
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
        except BaseException:
            # Nothing calls `close()` on a worker whose constructor raised,
            # so it stops what it started before it re-raises.
            # A `BaseException`, so the `SystemExit` of a SIGTERM
            # that lands while the actor is built cleans up too.
            self._stop_monitors()
            self._publish_exit()
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

        # These keys must match the ones `define_job_group` writes.
        # See the developer notes, Task flow.
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
            "start_time": _timestamp(),
        }
        self.client.map_set(
            f"{WORKER_INFO_PREFIX}{self.worker_id}",
            json.dumps(identity).encode("utf-8"),
        )

    def _publish_exit(self) -> None:
        """Record when this worker exited, once, as one JSON key."""
        if self._exit_published:
            return
        self._exit_published = True
        try:
            self.client.map_set(
                f"{WORKER_EXIT_PREFIX}{self.worker_id}",
                json.dumps({"exit_time": _timestamp()}).encode("utf-8"),
            )
        except Exception:
            # A worker on its way out cannot do more than say so.
            self.logger.exception("Failed to publish the exit of %s", self.worker_id)

    def _start_monitors(
        self, hostname: str, slurm_job_id: int, interval: float
    ) -> None:
        """Monitor this node and this job on it, unless a peer here already does."""
        # The counter hands out distinct values,
        # so exactly one worker per job per node sees 1 and takes both subjects.
        # The counter key holds the job id,
        # because counters never reset while the server runs,
        # and a node that a later pilot job lands on would otherwise get no sampler.
        # Two live jobs on one node both sample it,
        # which only adds points to the same host series.
        # See the developer notes, Monitoring.
        counter = f"host_monitor:{hostname}:{slurm_job_id}"
        if self.client.counter_get_next_value(counter) != 1:
            return

        self.logger.info("Monitoring host %s", hostname)
        self.monitors.append(
            start_host_monitor(self.client, hostname, interval, self.logger)
        )
        self.logger.info("Monitoring slurm job %s on %s", slurm_job_id, hostname)
        self.monitors.append(
            start_slurm_job_monitor(
                self.client, slurm_job_id, hostname, interval, self.logger
            )
        )

    def _get_actor_ctor_arg(self, key: str, default: Any) -> Any:
        """Read one cloudpickled constructor argument from the map."""
        try:
            value = self.client.map_get(key)
        except KeyError:
            # A missing key means the caller passed none.
            # See the developer notes, Task flow.
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
        """Stop the monitors, publish the exit, and close the connection and actor.

        Calls the actor's own `close()` if it has one.
        Clears `current_actor()` if it holds this worker's actor.
        """
        # Before the client, whose channel they use.
        self._stop_monitors()

        self._publish_exit()
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
        The command line entry point turns the SIGTERM
        that ends the job into a `SystemExit`,
        which this loop does not catch.
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
@click.option(
    "--pilot-job-event",
    type=click.Choice(["start", "exit"]),
    default=None,
    help="Publish that the pilot job started or exited, and start no worker.",
)
def slurm_pilot_worker(
    group: str,
    name: str,
    actor_class_name: str,
    server_address: str,
    work_dir: Path,
    python_paths_json: str,
    pilot_job_event: Literal["start", "exit"] | None,
) -> None:
    """Start a worker, or publish an event of the pilot job it runs in."""
    if pilot_job_event is not None:
        with DsServiceClient(server_address) as client:
            publish_pilot_job_event(client, name, pilot_job_event)
        return

    # Outside a Slurm job, the id is -1.
    slurm_job_id = int(os.environ.get("SLURM_JOB_ID", -1))
    hostname = socket.gethostname()
    pid = os.getpid()

    # Logs to the inherited stderr, which Slurm writes to a file.
    # See the developer notes, Slurm interaction.
    logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

    python_paths: list[str] = json.loads(python_paths_json)
    for path in python_paths:
        sys.path.insert(0, path)

    # Slurm ends the job with SIGTERM, and Python's default for it skips `finally`.
    # See the developer notes, Slurm interaction.
    signal.signal(signal.SIGTERM, _exit_on_sigterm)

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
