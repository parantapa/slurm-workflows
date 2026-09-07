"""Pilot workers for Slurm pilot."""

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

NEXT_TASK_RETRY_TIME_S: float = 0.1

# One JSON key per worker process, keyed on its worker id.
# Read by `swtop`; the fields are listed in `docs/concepts.md`.
WORKER_PROCESS_INFO_PREFIX = "worker_process_info:"


class PilotWorkerProcess:
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
    ):
        self.group = group
        self.name = name
        self.server_address = server_address
        self.work_dir = work_dir

        # `<worker-name>.<job>.<host>.<pid>`; the name carries the group.
        self.worker_id = "%s.%s.%s.%s" % (name, slurm_job_id, hostname, pid)
        self.logger = logging.getLogger("worker_process")
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
            # Nothing calls `close()` on a worker whose constructor raised.
            self._stop_monitors()
            self.client.close()
            raise

    def _build_actor(self, actor_class_name: str) -> Any | None:
        """Import and construct this group's actor, if it has one."""
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

    def _publish_identity(self, slurm_job_id: int, hostname: str, pid: int) -> None:
        """Record who this worker is in the key value store, as one JSON key."""
        identity = {
            "group": self.group,
            "name": self.name,
            "slurm_job_id": slurm_job_id,
            "hostname": hostname,
            "pid": pid,
        }
        self.client.map_set(
            f"{WORKER_PROCESS_INFO_PREFIX}{self.worker_id}",
            json.dumps(identity).encode("utf-8"),
        )

    def _start_monitors(
        self, hostname: str, slurm_job_id: int, interval: float
    ) -> None:
        """Take on monitoring this node and this job, if nobody else has.

        The worker a counter answers 1 to takes the subject, for good.
        """
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
        """Read one cloudpickled constructor argument out of the key value store.

        A missing key means none was passed.
        """
        try:
            value = self.client.map_get(key)
        except KeyError:
            return default
        return cloudpickle.loads(value)

    def _stop_monitors(self) -> None:
        """Stop whatever monitoring this worker took on.

        Idempotent, and safe on a half-built worker.
        """
        for monitor in self.monitors:
            monitor.stop()
        self.monitors.clear()

    def close(self):
        # Before the client, whose channel they are using.
        self._stop_monitors()

        self.client.close()
        if self.actor_instance is not None:
            if hasattr(self.actor_instance, "close"):
                self.actor_instance.close()
            self.actor_instance = None

    def main(self):
        self.logger.info("Starting worker: %s" % self.worker_id)

        while True:
            try:
                try:
                    task = self.client.task_get(self.worker_id, self.group)
                except NoTaskAvailable:
                    # An idle queue: sleep and ask again.
                    # A TimeoutError is a server problem, handled below.
                    time.sleep(NEXT_TASK_RETRY_TIME_S)
                    continue

                try:
                    self.logger.info(
                        "task_id=%s: Deserializing function and inputs ...",
                        task.task_id,
                    )
                    function = cloudpickle.loads(task.function)
                    if self.actor_instance is not None:
                        function = getattr(self.actor_instance, function)
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
                    self.client.task_done(task.task_id, self.worker_id, output)
            except Exception:
                self.logger.exception("Unexpected exception")

                # A refused connection returns at once, so without this
                # sleep the loop would spin on a core.
                time.sleep(NEXT_TASK_RETRY_TIME_S)


@click.command()
@click.option("--group", type=str, required=True, help="Worker group.")
@click.option("--name", type=str, required=True, help="Worker job name.")
@click.option(
    "--actor-class-name",
    type=str,
    required=True,
    help="Name for actor class in DS server store.",
)
@click.option("--server-address", type=str, required=True, help="Pilot server address.")
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
):
    """Start a slurm pilot worker."""
    slurm_job_id = int(os.environ.get("SLURM_JOB_ID", -1))
    hostname = socket.gethostname()
    pid = os.getpid()

    logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

    os.environ["PILOT_WORKER_NAME"] = name
    os.environ["PILOT_WORKER_GROUP"] = group
    os.environ["DS_SERVER_ADDRESS"] = server_address

    python_paths: list[str] = json.loads(python_paths_json)
    for path in python_paths:
        sys.path.insert(0, path)

    worker = PilotWorkerProcess(
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
