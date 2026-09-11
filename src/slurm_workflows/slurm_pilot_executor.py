"""The coordinator: worker groups, pilot jobs, and the tasks they run.

One executor per `ds-service` server.
`docs/explanation/about-the-pilot-job-model.md` explains the model.
"""

from __future__ import annotations

import re
import sys
import time
import json
import uuid
import pickle
import logging
import subprocess
from enum import Enum, auto
from types import TracebackType
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Callable, Iterable, Any, cast

import platformdirs
import cloudpickle
from typeguard import typechecked
from ds_service_client import DsServiceClient, TaskState

from .slurm_utils import (
    get_running_jobids,
    cancel_jobs,
    submit_sbatch_job,
    SlurmJob,
)

from .utils import (
    RemoteExecutionError,
    LOG_FORMAT,
    LOG_LEVEL,
)

from .templates import render_template

NoOutput = object()

# One JSON key per submitted pilot job, keyed on the worker name.
# `swtop` reads it.
# `docs/reference/executor.md` lists the fields.
WORKER_JOB_INFO_PREFIX = "worker_job_info:"

# What `wait` and `as_completed` work through, for `swtop` to draw.
# Each call overwrites the key.
# The series under the id it carries holds the count completed so far.
PROGRESS_DISPLAY_KEY = "progress_display"
PROGRESS_SERIES_PREFIX = "progress:"

# How often the executor appends the count while tasks come back.
PROGRESS_INTERVAL_S: float = 1.0


class RaiseOnError(Enum):
    """What `as_completed` and `wait` do about a task that fails.

    A failure is a task whose worker raised,
    one canceled on the queue server,
    or one the server does not know.
    A pending task is also a failure
    when its queues have no pilot job left to run it.
    The executor reports every failure as it meets one, whatever the value.
    The value decides only whether an exception follows.
    """

    # Stop at the first failure.
    RAISE_ON_FIRST_ERROR = auto()

    # Wait for every task that can still finish, then raise for all at once.
    # `as_completed` treats this as RAISE_ON_FIRST_ERROR.
    RAISE_AFTER_COMPLETED = auto()

    # Report the failures and return.
    # The caller reads `task.output`.
    RAISE_NEVER = auto()


# How many failures a deferred exception names before it stops listing them.
MAX_REPORTED_ERRORS = 5

# What you can name an executor:
# the intersection of what is safe in a task id, a logger name,
# a directory name and a Slurm job name.
EXECUTOR_NAME_REGEX = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
MIN_EXECUTOR_NAME_LEN: int = 3

POLL_INTERVAL_S: float = 0.1

# How often `_as_completed` re-checks that pending tasks still have a pilot job.
# The interval is well above POLL_INTERVAL_S:
# each check costs an `squeue` call.
LIVE_QUEUE_CHECK_INTERVAL_S: float = 60.0


@dataclass
class Task:
    """A submitted task, and what it returned once it finished.

    `output` is `NoOutput` until then.
    Afterward `output` holds the return value,
    or a `RemoteExecutionError` if the worker raised.
    """

    task_id: str
    queue: list[str]
    priority: float
    function: Callable | str
    input: tuple
    output: Any
    _task_name: str | None = None

    @property
    def task_name(self) -> str | None:
        """The name `SlurmPilotExecutor.set_task_name` set, or None."""
        return self._task_name


@dataclass
class WorkerGroup:
    """A named recipe for starting workers, and the queue they serve.

    The name is also the queue name:
    only workers of this group serve a task on the queue `name`.
    """

    name: str
    sbatch_args: list[str]
    is_batch_worker: bool
    worker_exe: str
    actor_class_name: str
    setup_script: str
    python_paths: list[str]
    workers: dict[str, SlurmJob] = field(default_factory=dict, compare=False)
    next_worker_index: int = field(default=0, compare=False)


@dataclass
class _Progress:
    """One `wait` or `as_completed` call, counted into a time series."""

    client: DsServiceClient
    progress_id: str
    total: int
    _last_sent: float = field(default=0.0, compare=False)

    def record(self, done: int) -> None:
        """Append `done`, unless the series took a value recently."""
        now = time.monotonic()
        if done < self.total and now - self._last_sent < PROGRESS_INTERVAL_S:
            return
        self.append(done)

    def append(self, done: int) -> None:
        """Append `done` to the series now, and ignore the interval."""
        self._last_sent = time.monotonic()
        self.client.time_series_append(
            f"{PROGRESS_SERIES_PREFIX}{self.progress_id}",
            float(done),
            datetime.now(timezone.utc).isoformat(),
        )


class SlurmPilotExecutor:
    """Runs Python callables on a Slurm cluster through a pool of pilot workers.

    Give each executor a `ds-service` server of its own:
    everything on a server belongs to one run.
    Use the executor as a context manager, or call `close()`,
    so the executor cancels the pilot jobs when the run ends.
    """

    @typechecked
    def __init__(
        self,
        name: str,
        server_address: str,
        work_dir: Path | str | None = None,
    ) -> None:
        """Connect to a queue server and open a work directory for this run.

        `name` prefixes task ids, worker job names and the log,
        so two executors on one cluster need two names.
        `name` must match `[A-Za-z][A-Za-z0-9_-]*`
        and hold at least `MIN_EXECUTOR_NAME_LEN` characters.
        Anything else raises `ValueError`.
        `work_dir` defaults to a timestamped directory in the user cache.
        The executor creates that directory if it does not exist.
        """
        if len(name) < MIN_EXECUTOR_NAME_LEN:
            raise ValueError(
                f"Executor name {name!r} is shorter than "
                f"{MIN_EXECUTOR_NAME_LEN} characters"
            )
        if EXECUTOR_NAME_REGEX.fullmatch(name) is None:
            raise ValueError(
                f"Executor name {name!r} must start with a letter "
                f"and hold only letters, digits, '_' and '-'"
            )

        self.name = name
        self.next_task_index = 0

        self.server_address = server_address
        self.client = DsServiceClient(server_address)

        if work_dir is None:
            now = datetime.now().isoformat()
            work_dir = (
                platformdirs.user_cache_path(appname="slurm-workflows") / name / now
            )
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        print(f"work directory: '{self.work_dir}'")

        # A logger of this executor's own, keyed on its name.
        self.logger = logging.getLogger(f"slurm_workflows.executor.{self.name}")
        self.logger.setLevel(LOG_LEVEL)
        # These records belong in the work dir, not on the root logger.
        self.logger.propagate = False
        handler = logging.FileHandler(self.work_dir / "executor.log", delay=True)
        handler.setLevel(LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        # Detached and closed by `close()`, which releases the log file.
        self._log_handler: logging.FileHandler | None = handler

        self.groups: dict[str, WorkerGroup] = {}

    @typechecked
    def define_worker(
        self,
        name: str,
        sbatch_args: list[str],
        setup_script: str = "",
        worker_exe: str = "slurm-pilot-worker",
        is_batch_worker: bool = False,
        actor_class_name: str | None = None,
        actor_class_args: list[Any] | None = None,
        actor_class_kwargs: dict[str, Any] | None = None,
        python_paths: list[str | Path] | None = None,
        add_cwd_to_python_path: bool = True,
    ) -> None:
        """Register a worker group. `scale_workers` submits the jobs.

        `name` is also the queue name.
        `setup_script` is shell text, not a path.
        The executor inlines it into each generated worker script.
        The actor arguments need an `actor_class_name` to construct,
        and raise `ValueError` without one.
        A second, identical definition of a group does nothing.
        A definition that differs from the first one raises `AssertionError`.
        """
        python_str_paths: list[str] = []
        if python_paths is not None:
            for path in python_paths:
                python_str_paths.append(str(path))
        if add_cwd_to_python_path:
            python_str_paths.append(str(Path.cwd()))

        if actor_class_name is None:
            if actor_class_args is not None or actor_class_kwargs is not None:
                raise ValueError(
                    "actor_class_args and actor_class_kwargs "
                    "need an actor_class_name to construct"
                )
            actor_class_name = ""

        group = WorkerGroup(
            name=name,
            sbatch_args=sbatch_args,
            worker_exe=worker_exe,
            is_batch_worker=is_batch_worker,
            actor_class_name=actor_class_name,
            setup_script=setup_script,
            python_paths=python_str_paths,
        )

        if group.name in self.groups:
            assert self.groups[group.name] == group
        else:
            self.groups[group.name] = group

        # These go into the store, cloudpickled and keyed on the group name.
        # Each worker reads them at startup.
        if actor_class_args is not None:
            self.client.map_set(
                f"actor_class_args:{name}",
                cloudpickle.dumps(actor_class_args, protocol=pickle.HIGHEST_PROTOCOL),
            )
        if actor_class_kwargs is not None:
            self.client.map_set(
                f"actor_class_kwargs:{name}",
                cloudpickle.dumps(actor_class_kwargs, protocol=pickle.HIGHEST_PROTOCOL),
            )

    def _add_worker(self, group: WorkerGroup) -> None:
        """Render one worker's scripts and submit the pilot job that runs them."""
        worker_index = group.next_worker_index
        group.next_worker_index += 1
        worker_name = f"{self.name}.worker.{group.name}.{worker_index}"

        worker_script = render_template(
            "slurm_pilot:worker_script",
            group=group.name,
            name=worker_name,
            server_address=self.server_address,
            worker_exe=group.worker_exe,
            work_dir=self.work_dir,
            python_paths_json=json.dumps(group.python_paths),
            setup_script=group.setup_script,
            actor_class_name=group.actor_class_name,
        )
        worker_script_path = self.work_dir / f"{worker_name}.sh"
        worker_script_path.write_text(worker_script)
        worker_script_path.chmod(0o755)

        worker_sbatch_script = render_template(
            "slurm_pilot:worker_sbatch_script",
            name=worker_name,
            work_dir=self.work_dir,
            is_batch_worker=group.is_batch_worker,
            worker_script_path=worker_script_path,
        )

        self.logger.info("Starting worker %s", worker_name)
        try:
            slurm_job = submit_sbatch_job(
                name=worker_name,
                sbatch_args=group.sbatch_args,
                script=worker_sbatch_script,
                work_dir=self.work_dir,
            )
            group.workers[worker_name] = slurm_job
            self._publish_worker_job(worker_name, group.name, slurm_job.job_id)
        except subprocess.CalledProcessError as cp:
            print(f"Failed to submit slurm job: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stderr)
            raise cp

    def _publish_worker_job(
        self, worker_name: str, group_name: str, slurm_job_id: int
    ) -> None:
        """Record one submitted pilot job in the key value store, as JSON."""
        info = {
            "name": worker_name,
            "group": group_name,
            "slurm_job_id": slurm_job_id,
            "submit_time": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        self.client.map_set(
            f"{WORKER_JOB_INFO_PREFIX}{worker_name}",
            json.dumps(info).encode("utf-8"),
        )

    @typechecked
    def scale_workers(self, name: str, count: int) -> None:
        """Submit or cancel pilot jobs so the group holds `count` of them.

        Returns as soon as `sbatch` accepts the jobs, not when they start.
        """
        assert name in self.groups, "Unknown worker type"

        group = self.groups[name]
        if len(group.workers) < count:
            to_hire = count - len(group.workers)
            for _ in range(to_hire):
                self._add_worker(group)

        if len(group.workers) > count:
            to_retire = len(group.workers) - count

            try:
                running_jobids = get_running_jobids()
            except subprocess.CalledProcessError as cp:
                print(
                    f"Failed to get running slurm job ids: returncode={cp.returncode}"
                )
                if cp.stdout.strip():
                    print(cp.stdout)
                if cp.stderr.strip():
                    print(cp.stderr)
                raise RuntimeError("Failed to get running slurm job ids")
            except Exception:
                raise RuntimeError("Failed to get running slurm job ids")

            to_cancel_jobids = []
            for _ in range(to_retire):
                _, worker = group.workers.popitem()
                self.logger.info("Canceling worker: %s", worker.name)
                if worker.job_id in running_jobids:
                    to_cancel_jobids.append(worker.job_id)

            if not to_cancel_jobids:
                return

            try:
                cancel_jobs(to_cancel_jobids)
            except subprocess.CalledProcessError as cp:
                print(f"Failed to cancel slurm jobs: returncode={cp.returncode}")
                if cp.stdout.strip():
                    print(cp.stdout)
                if cp.stderr.strip():
                    print(cp.stderr)
                raise RuntimeError("Failed to cancel slurm jobs")
            except Exception:
                raise RuntimeError("Failed to cancel slurm jobs")

    def _submit(
        self,
        queue: list[str],
        fn: Callable | str,
        *args,
        **kwargs,
    ) -> Task:
        """Enqueue one task on the given queues and return its handle."""
        # Negated: ds-service serves the highest priority first,
        # and serves one queue oldest first.
        priority = -time.time()
        function_bytes = cloudpickle.dumps(fn, protocol=pickle.HIGHEST_PROTOCOL)
        input_bytes = cloudpickle.dumps(
            (args, kwargs), protocol=pickle.HIGHEST_PROTOCOL
        )

        task_id = f"{self.name}.task.{self.next_task_index}"
        self.next_task_index += 1

        task = Task(
            task_id=task_id,
            queue=queue,
            priority=priority,
            function=fn,
            input=(args, kwargs),
            output=NoOutput,
        )

        self.client.task_add(
            task_id=task_id,
            queue=queue,
            priority=priority,
            function=function_bytes,
            input=input_bytes,
        )
        return task

    @typechecked
    def set_task_name(self, task: Task, name: str) -> None:
        """Give `task` a name, on the queue server as well as locally.

        The name is for whoever reads the queue.
        Nothing here dispatches on it.
        """
        # Server first: a failed write leaves the task unnamed on both sides.
        self.client.map_set(f"task_name:{task.task_id}", name.encode("utf-8"))
        task._task_name = name

    @typechecked
    def submit(
        self, queue: str | list[str], fn: Callable | str, *args: Any, **kwargs: Any
    ) -> Task:
        """Enqueue one task and return its handle immediately.

        `fn` is a callable, or a method name for a group with an actor.
        This method does not check the queue name.
        `wait` and `as_completed` report a task on a queue no group serves.
        The queue server dispatches tasks on one queue oldest first.
        """
        if isinstance(queue, str):
            queue = [queue]

        return self._submit(queue, fn, *args, **kwargs)

    def _warn(self, message: str) -> None:
        """Report one failure on stderr as it happens."""
        print(f"warning: {message}", file=sys.stderr, flush=True)

    def _as_completed(
        self, tasks: list[Task], raise_on_error: RaiseOnError
    ) -> Iterable[Task]:
        """Yield tasks as they finish, and apply `raise_on_error` to failures."""
        pending: list[Task] = []
        finished: list[Task] = []
        for task in tasks:
            if task.output is NoOutput:
                pending.append(task)
            else:
                finished.append(task)

        # Every failure met on the way.
        # Only a deferred raise reads this list.
        errors: list[str] = []
        # Tasks, not messages: one message can cover a whole queue's worth.
        failures = 0

        def failed(message: str, tasks: int = 1) -> None:
            """Warn about a failure, and raise now if that is the policy."""
            nonlocal failures
            self._warn(message)
            errors.append(message)
            failures += tasks
            if raise_on_error is RaiseOnError.RAISE_ON_FIRST_ERROR:
                raise RuntimeError(message)

        def drop(unrunnable: list[Task], message: str) -> list[Task]:
            """Report tasks that can never finish, and stop waiting on them."""
            failed(message, len(unrunnable))
            unrunnable_ids = {task.task_id for task in unrunnable}
            return [task for task in pending if task.task_id not in unrunnable_ids]

        # Before the first yield.
        if pending:
            starved, message = self._starved_tasks(pending)
            if starved:
                pending = drop(starved, message)

        yield from finished

        # One interval away, not immediate:
        # a caller can submit before any worker exists,
        # so a queue with no job yet is normal.
        next_liveness_check = time.monotonic() + LIVE_QUEUE_CHECK_INTERVAL_S

        while pending:
            # One request for every pending task.
            # The server answers in the order sent.
            states = self.client.task_get_status([t.task_id for t in pending])
            states = cast(list[TaskState], states)

            next_pending: list[Task] = []
            completed = 0
            for task, state in zip(pending, states):
                if state == TaskState.Complete:
                    output = self.client.task_get_output(task.task_id)
                    task.output = cloudpickle.loads(output)
                    completed += 1
                    if isinstance(task.output, RemoteExecutionError):
                        # The task finished.
                        # What it produced is the failure.
                        failed(
                            f"Task {task.task_id} failed on its worker: "
                            f"{task.output.error} "
                            f"(error_id={task.output.error_id})"
                        )
                    yield task
                elif state == TaskState.Canceled:
                    # Something outside this run canceled the task.
                    # The server never dispatches it again.
                    failed(
                        f"Task {task.task_id} was canceled on the task queue "
                        f"server, so it will never produce an output"
                    )
                elif state == TaskState.Undefined:
                    failed(f"Task {task.task_id} is unknown to the task queue server")
                else:
                    next_pending.append(task)

            pending = next_pending

            if pending and time.monotonic() >= next_liveness_check:
                stranded, message = self._stranded_tasks(pending)
                if stranded:
                    pending = drop(stranded, message)
                next_liveness_check = time.monotonic() + LIVE_QUEUE_CHECK_INTERVAL_S

            if pending and not completed:
                time.sleep(POLL_INTERVAL_S)

        if errors and raise_on_error is RaiseOnError.RAISE_AFTER_COMPLETED:
            raise RuntimeError(self._error_summary(errors, failures, len(tasks)))

    @staticmethod
    def _error_summary(errors: list[str], failures: int, num_tasks: int) -> str:
        """One message that names a few failures and counts the tasks."""
        shown = "; ".join(errors[:MAX_REPORTED_ERRORS])
        if len(errors) > MAX_REPORTED_ERRORS:
            shown += f"; and {len(errors) - MAX_REPORTED_ERRORS} more"
        return f"{failures} of {num_tasks} tasks did not succeed: {shown}"

    @typechecked
    def as_completed(
        self,
        tasks: Iterable[Task],
        desc: str,
        unit: str = "task",
        raise_on_error: RaiseOnError = RaiseOnError.RAISE_ON_FIRST_ERROR,
    ) -> Iterable[Task]:
        """Yield tasks as their results arrive.

        `desc` and `unit` label the progress `swtop` draws for this call.
        """
        tasks = list(tasks)
        if raise_on_error is RaiseOnError.RAISE_AFTER_COMPLETED:
            raise_on_error = RaiseOnError.RAISE_ON_FIRST_ERROR

        progress = self._publish_progress(desc, unit, len(tasks))
        return self._counted(self._as_completed(tasks, raise_on_error), progress)

    @typechecked
    def wait(
        self,
        tasks: Iterable[Task],
        desc: str,
        unit: str = "task",
        raise_on_error: RaiseOnError = RaiseOnError.RAISE_ON_FIRST_ERROR,
    ) -> None:
        """Block until every task is done.

        `desc` and `unit` label the progress `swtop` draws for this call.
        """
        tasks = list(tasks)
        progress = self._publish_progress(desc, unit, len(tasks))
        # Not through `as_completed`, which cannot defer an exception.
        for _ in self._counted(self._as_completed(tasks, raise_on_error), progress):
            pass

    def _publish_progress(self, desc: str, unit: str, total: int) -> _Progress:
        """Announce what this call works through, and start its series."""
        progress = _Progress(
            client=self.client,
            progress_id=str(uuid.uuid4()),
            total=total,
        )
        self.client.map_set(
            PROGRESS_DISPLAY_KEY,
            json.dumps(
                {
                    "progress_id": progress.progress_id,
                    "desc": desc,
                    "unit": unit,
                    "total": total,
                }
            ).encode("utf-8"),
        )
        progress.append(0)
        return progress

    @staticmethod
    def _counted(tasks: Iterable[Task], progress: _Progress) -> Iterable[Task]:
        """Pass tasks through, and count how many come back."""
        done = 0
        try:
            for task in tasks:
                done += 1
                progress.record(done)
                yield task
        finally:
            progress.append(done)

    def _live_queues(self, queues: Iterable[str] | None = None) -> set[str]:
        """Queues `squeue` still lists a job for, pending or running.

        Whatever `get_running_jobids` raises propagates.
        """
        job_ids = get_running_jobids()

        groups = self.groups.values()
        if queues is not None:
            # Only the names given.
            # A name no group has is absent from the answer.
            wanted = set(queues)
            groups = [g for g in groups if g.name in wanted]

        return {
            group.name
            for group in groups
            if any(worker.job_id in job_ids for worker in group.workers.values())
        }

    def _starved_tasks(self, pending: list[Task]) -> tuple[list[Task], str]:
        """Pending tasks with no worker ever started for them, and a message."""
        # This executor's own bookkeeping, not a question to the cluster:
        # the check must work before any job can start.
        started = {name for name, group in self.groups.items() if group.workers}

        starved = [task for task in pending if not set(task.queue) & started]
        if not starved:
            return [], ""

        queues = sorted({q for task in starved for q in task.queue})
        return starved, (
            f"{len(starved)} of {len(pending)} pending tasks are on queues with "
            f"no worker started: {queues}. "
            f"Call scale_workers() for a worker group of that name "
            f"before waiting on them."
        )

    def _stranded_tasks(self, pending: list[Task]) -> tuple[list[Task], str]:
        """Pending tasks whose queues have no pilot job left, and a message."""
        try:
            live = self._live_queues({q for task in pending for q in task.queue})
        except (subprocess.SubprocessError, OSError):
            # Unreachable `squeue` leaves liveness unknown, not dead,
            # so this gives up no task, and the next interval retries.
            self.logger.warning(
                "Could not check whether queues are still live; "
                "will retry at the next interval",
                exc_info=True,
            )
            return [], ""

        stranded = [task for task in pending if not set(task.queue) & live]
        if not stranded:
            return [], ""

        queues = sorted({q for task in stranded for q in task.queue})
        return stranded, (
            f"{len(stranded)} of {len(pending)} pending tasks are on queues with "
            f"no live pilot job, so they can never run: {queues}. "
            f"Scale up a worker group named after one of those queues, "
            f"or cancel the wait."
        )

    def _cleanup_all_workers(self) -> None:
        """Cancel every pilot job still on the cluster. Reports, never raises."""
        try:
            job_ids = get_running_jobids()
        except subprocess.CalledProcessError as cp:
            print(f"Failed to get running slurm job ids: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stderr)
            job_ids = None
        except Exception:
            self.logger.exception("Failed to get running slurm job ids")
            job_ids = None

        if job_ids is None:
            return

        to_cancel_jobids = []
        for group in self.groups.values():
            for worker in group.workers.values():
                if worker.job_id in job_ids:
                    to_cancel_jobids.append(worker.job_id)

        if not to_cancel_jobids:
            return

        try:
            cancel_jobs(to_cancel_jobids)
        except subprocess.CalledProcessError as cp:
            print(f"Failed to cancel slurm jobs: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stderr)
        except Exception:
            self.logger.exception("Failed to cancel slurm jobs")

    def _close_log_handler(self) -> None:
        """Detach this executor's log handler and close its file. Idempotent."""
        handler = self._log_handler
        if handler is None:
            return

        # Cleared first, so a later failure cannot double-close it.
        self._log_handler = None
        self.logger.removeHandler(handler)
        handler.close()

    def close(self) -> None:
        """Cancel every pilot job and close the queue-server connection.

        The executor is spent afterward.
        Python calls this at the end of a `with` block.
        """
        self._cleanup_all_workers()
        for group in self.groups.values():
            group.workers.clear()

        self.client.close()
        self._close_log_handler()

    def stop(self) -> None:
        """Cancel every pilot job, and leave the executor usable."""
        self._cleanup_all_workers()
        for group in self.groups.values():
            group.workers.clear()

    def __enter__(self) -> "SlurmPilotExecutor":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
