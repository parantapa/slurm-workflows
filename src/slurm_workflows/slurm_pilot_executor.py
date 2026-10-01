"""The executor: job groups, pilot jobs, and the tasks they run."""

from __future__ import annotations

import re
import os
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
from typing import Callable, Iterable, Iterator, Any, cast

import platformdirs
import cloudpickle
from typeguard import typechecked
from ds_service_client import DsServiceClient, TaskState, NoTaskAvailable

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

from .slurm_pilot_worker import (
    current_actor,
    RESTART_EXIT_CODE,
    RESTART_GENERATION_PREFIX,
    WORKER_EXIT_PREFIX,
    WORKER_INFO_PREFIX,
)

# What a `Task`'s `output` holds until the task finishes.
# A task that was canceled, is unknown to the server,
# or has no pilot job left to run it keeps it.
NoOutput = object()

# One JSON key per submitted pilot job, keyed on the job name.
# `swtop` reads it.
# `docs/reference/what-a-run-publishes.md` lists the fields.
PILOT_JOB_INFO_PREFIX = "pilot_job_info:"

# What `wait` and `as_completed` work through, for `swtop` to draw.
# Each call overwrites the key.
# The time series under the id it carries holds the count completed so far.
PROGRESS_DISPLAY_KEY = "progress_display"
PROGRESS_SERIES_PREFIX = "progress:"

# How often the executor appends the count while tasks come back.
PROGRESS_INTERVAL_S: float = 1.0

# The queue one `mapreduce` or `map` call puts its items on,
# and the id of each item task on that queue.
# The `mapreduce` and `map` segments keep these ids clear of `<name>.task.<n>`,
# and clear of each other.
# The token keeps two runs of one executor name clear of each other:
# the index restarts at 0 in each executor,
# and `task_add` refuses a duplicate id.
MAPREDUCE_QUEUE_TEMPLATE = "{name}.mapreduce.{index}.{token}"
MAP_QUEUE_TEMPLATE = "{name}.map.{index}.{token}"
MAPREDUCE_ITEM_TEMPLATE = "{queue}.item.{index}"

# How many hex characters of a UUID4 an item queue name carries.
MAPREDUCE_TOKEN_LEN: int = 8

# What an item task carries in place of a function,
# and what it records as its output.
# Only a map task claims an item task,
# and that task reads the item out of the task's input,
# so nothing ever deserializes either field.
NO_FUNCTION = b""
NO_TASK_OUTPUT = b""

# How `ds-service` begins the output of a task it failed
# because a task it waits on failed.
# The rest of that output names the failed task.
# The output is plain text, not a cloudpickle.
DEPENDENCY_FAILED_PREFIX = b"Dependency failed"


class RaiseOnError(Enum):
    """What `as_completed` and `wait` do about a task that fails.

    A failure is a task whose worker raised,
    or one canceled on the server.
    A task whose parent task failed or was canceled is a failure too,
    and so is a task the server does not know.
    A pending task is also a failure
    when its queues, or those of an unfinished ancestor,
    have no pilot job left to run it.
    The executor reports every failure as it meets one, whatever the value.
    The value decides only whether an exception follows.
    """

    RAISE_ON_FIRST_ERROR = auto()
    """Stop at the first failure."""

    RAISE_AFTER_COMPLETED = auto()
    """Wait for every task that can still finish, then raise for all at once.

    `as_completed` treats this as `RAISE_ON_FIRST_ERROR`.
    """

    RAISE_NEVER = auto()
    """Report the failures and return.

    The caller reads `task.output`.
    """


# How many failures a deferred exception names before it stops listing them.
MAX_REPORTED_ERRORS = 5

# The names an executor accepts:
# the intersection of what is safe in a task id, a logger name,
# a directory name and a Slurm job name.
EXECUTOR_NAME_REGEX = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
MIN_EXECUTOR_NAME_LEN: int = 3

# How long `_as_completed` sleeps between two status polls
# that brought back nothing.
POLL_INTERVAL_S: float = 0.1

# How often `_as_completed` re-checks that pending tasks still have a pilot job.
# The interval is well above `POLL_INTERVAL_S`:
# each check costs an `squeue` call.
LIVE_QUEUE_CHECK_INTERVAL_S: float = 60.0

# How long `restart_jobs` sleeps between two looks at the workers it waits on.
RESTART_POLL_INTERVAL_S: float = 1.0

# How often `restart_jobs` asks `squeue` which pilot jobs are still live.
# The interval is above `RESTART_POLL_INTERVAL_S`,
# since each check costs an `squeue` call.
RESTART_LIVE_CHECK_INTERVAL_S: float = 10.0

# How many workers a restart timeout or a restart warning names
# before it stops listing them.
MAX_REPORTED_WORKERS = 5


@dataclass
class Task:
    """A submitted task, and what it returned once it finished.

    `output` is `NoOutput` until then.
    Afterward `output` holds the return value,
    or a `RemoteExecutionError` if the worker raised
    or a parent task failed.
    `parent_task_ids` holds the ids of the tasks this one waits on.
    """

    task_id: str
    queue: list[str]
    priority: float
    function: Callable | str
    input: tuple[tuple[Any, ...], dict[str, Any]]
    output: Any
    parent_task_ids: list[str] = field(default_factory=list)
    _task_name: str | None = None
    # The parents themselves, so a wait can check their queues too.
    _parents: list["Task"] = field(default_factory=list, repr=False, compare=False)

    # Read-only: the map holds the other copy,
    # so a setter here can rename the task in this process alone.
    @property
    def task_name(self) -> str | None:
        """The name `SlurmPilotExecutor.set_task_name` set, or None."""
        return self._task_name


@dataclass
class JobGroup:
    """A named recipe for pilot jobs, and the queue their workers serve.

    `scale_jobs` sets how many pilot jobs the job group has.
    The sbatch arguments and `is_batch_worker`
    decide how many workers those jobs start.
    The name is also the queue name:
    only workers of this job group serve a task on the queue `name`.
    """

    name: str
    sbatch_args: list[str]
    is_batch_worker: bool
    worker_exe: str
    actor_class_name: str
    setup_script: str
    python_paths: list[str]
    # Runtime state, left out of the equality
    # that `define_job_group` checks a redefinition with.
    jobs: dict[str, SlurmJob] = field(default_factory=dict, compare=False)
    next_job_index: int = field(default=0, compare=False)


@dataclass
class _Progress:
    """One `wait` or `as_completed` call, counted into a time series."""

    client: DsServiceClient
    progress_id: str
    total: int
    _last_sent: float = field(default=0.0, compare=False)

    def record(self, done: int) -> None:
        """Append `done`, unless the time series took a value recently."""
        now = time.monotonic()
        # The final count always goes out, whatever the interval.
        if done < self.total and now - self._last_sent < PROGRESS_INTERVAL_S:
            return
        self.append(done)

    def append(self, done: int) -> None:
        """Append `done` to the time series now, and ignore the interval."""
        self._last_sent = time.monotonic()
        self.client.time_series_append(
            f"{PROGRESS_SERIES_PREFIX}{self.progress_id}",
            float(done),
            datetime.now(timezone.utc).isoformat(),
        )


def _resolve_map_method(name: str, what: str) -> Callable:
    """Look one map method name up on the actor of the worker that runs it."""
    actor = current_actor()
    if actor is None:
        raise RuntimeError(
            f"{what} map_fn names the method {name!r}, "
            f"but the worker running this task has no actor"
        )
    return getattr(actor, name)


def _mapreduce_task(
    mr_queue: str,
    map_fn: Callable | str,
    reduce_fn: Callable,
    init: Any,
    map_args: tuple[Any, ...],
    map_kwargs: dict[str, Any],
    reduce_args: tuple[Any, ...],
    reduce_kwargs: dict[str, Any],
) -> Any:
    """Map and fold every item this task claims from `mr_queue`."""
    # One worker runs one task at a time,
    # so its id names this map task as well.
    worker_id = os.environ["PILOT_WORKER_ID"]

    # Once, not once per item:
    # the worker keeps its actor until it exits,
    # so the bound method is the same for every item.
    if isinstance(map_fn, str):
        map_fn = _resolve_map_method(map_fn, "mapreduce")

    result = init

    # A client of this task's own.
    # `DsServiceClient()` reads the address the worker put in the environment.
    # See Mapreduce and map in the developer notes.
    with DsServiceClient() as client:
        while True:
            # No retry on `TimeoutError`: `task_get` is not idempotent.
            # The deadline can fire after the server recorded the claim,
            # and a retry then skips that item, which stays `Running` for good.
            # The error propagates instead, and the wait raises.
            try:
                item_task = client.task_get(worker_id, mr_queue)
            except NoTaskAvailable:
                # Every item was on the queue before this task started,
                # so nothing left to claim means nothing left at all.
                return result

            item = cloudpickle.loads(item_task.input)
            value = map_fn(item, *map_args, **map_kwargs)
            result = reduce_fn(result, value, *reduce_args, **reduce_kwargs)

            # After the fold, so an item goes Finished only after this task counts it.
            client.task_done(item_task.task_id, worker_id, NO_TASK_OUTPUT)


def _map_task(
    item_queue: str,
    map_fn: Callable | str,
    map_args: tuple[Any, ...],
    map_kwargs: dict[str, Any],
) -> list[tuple[str, Any]]:
    """Map the items this task claims, as `(item task id, value)` pairs."""
    # Everything `_mapreduce_task` says about the worker id, the method name,
    # the client and the retry holds here too.
    worker_id = os.environ["PILOT_WORKER_ID"]

    if isinstance(map_fn, str):
        map_fn = _resolve_map_method(map_fn, "map")

    values: list[tuple[str, Any]] = []

    with DsServiceClient() as client:
        while True:
            try:
                item_task = client.task_get(worker_id, item_queue)
            except NoTaskAvailable:
                return values

            item = cloudpickle.loads(item_task.input)
            values.append((item_task.task_id, map_fn(item, *map_args, **map_kwargs)))

            # The value travels home in this task's output, not the item task's.
            # See Mapreduce and map in the developer notes.
            client.task_done(item_task.task_id, worker_id, NO_TASK_OUTPUT)


# `docs/explanation/pilot-job-model.md` explains the model this class implements.
class SlurmPilotExecutor:
    """Runs Python callables on a Slurm cluster through a pool of workers.

    Each executor needs a `ds-service` server of its own,
    because everything on a server belongs to one run.
    The executor cancels its pilot jobs in `close()` and `stop()`,
    and `scale_jobs` cancels those above the count it is given.
    A `with` block calls `close()` at its end.

    The constructor opens a client for the `ds-service` server at `server_address`,
    and opens a work dir for this run.
    The client connects on the first call that needs it.
    `name` prefixes task ids and pilot job names, and names the logger,
    so two executors on one cluster need two names.
    `name` must match `[A-Za-z][A-Za-z0-9_-]*`
    and hold at least `MIN_EXECUTOR_NAME_LEN` characters,
    or the constructor raises `ValueError`.
    `work_dir` defaults to a timestamped directory in the user cache.
    The executor creates the work dir if it does not exist,
    prints its path on stdout, and logs to `executor.log` in it.
    """

    @typechecked
    def __init__(
        self,
        name: str,
        server_address: str,
        work_dir: Path | str | None = None,
    ) -> None:
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
        self.next_mapreduce_index = 0
        self.next_map_index = 0

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

        # Keyed on the name.
        # See Logging in the developer notes.
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

        self.groups: dict[str, JobGroup] = {}

    @typechecked
    def define_job_group(
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
        """Register a job group, without submitting any pilot job.

        `scale_jobs` submits the jobs.
        `name` is also the queue name.
        A second, identical definition of a job group does nothing.
        A definition that differs from the first one raises `AssertionError`.

        `actor_class_name` is a dotted `module.Class` path
        that each worker imports and constructs at startup.
        The actor arguments need an `actor_class_name` to construct,
        and raise `ValueError` without one.
        Actor arguments a later call passes replace the earlier ones,
        even for an otherwise identical definition.

        `setup_script` is shell text, not a path.
        The executor inlines it into each generated worker script.
        Nothing checks it.
        `is_batch_worker` runs one worker in the batch script itself,
        rather than one per Slurm task under `srun`.
        `python_paths` go on the front of each worker's `sys.path`, in order.
        `add_cwd_to_python_path` puts the driver's current directory
        as it is at this call ahead of them.
        """
        # In the order the worker puts them on `sys.path`.
        python_str_paths: list[str] = []
        if add_cwd_to_python_path:
            python_str_paths.append(str(Path.cwd()))
        if python_paths is not None:
            for path in python_paths:
                python_str_paths.append(str(path))

        if actor_class_name is None:
            if actor_class_args is not None or actor_class_kwargs is not None:
                raise ValueError(
                    "actor_class_args and actor_class_kwargs "
                    "need an actor_class_name to construct"
                )
            actor_class_name = ""

        group = JobGroup(
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

        # These go into the map, cloudpickled and keyed on the job group name.
        # Each worker reads them at startup, under the same keys.
        # See Task flow in the developer notes.
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

    def _add_job(self, group: JobGroup) -> None:
        """Render one pilot job's scripts and submit the pilot job that runs them."""
        job_index = group.next_job_index
        group.next_job_index += 1
        job_name = f"{self.name}.job.{group.name}.{job_index}"

        worker_script = render_template(
            "slurm_pilot:worker_script",
            group=group.name,
            name=job_name,
            server_address=self.server_address,
            worker_exe=group.worker_exe,
            work_dir=self.work_dir,
            python_paths_json=json.dumps(group.python_paths),
            setup_script=group.setup_script,
            actor_class_name=group.actor_class_name,
            restart_exit_code=RESTART_EXIT_CODE,
        )
        worker_script_path = self.work_dir / f"{job_name}.sh"
        worker_script_path.write_text(worker_script)
        worker_script_path.chmod(0o755)

        worker_sbatch_script = render_template(
            "slurm_pilot:worker_sbatch_script",
            name=job_name,
            work_dir=self.work_dir,
            is_batch_worker=group.is_batch_worker,
            worker_script_path=worker_script_path,
        )

        self.logger.info("Starting pilot job %s", job_name)
        try:
            slurm_job = submit_sbatch_job(
                name=job_name,
                sbatch_args=group.sbatch_args,
                script=worker_sbatch_script,
                work_dir=self.work_dir,
            )
            group.jobs[job_name] = slurm_job
            self._publish_pilot_job(job_name, group.name, slurm_job.job_id)
        except subprocess.CalledProcessError as cp:
            print(f"Failed to submit slurm job: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stderr)
            raise cp

    def _publish_pilot_job(
        self, job_name: str, group_name: str, slurm_job_id: int
    ) -> None:
        """Record one submitted pilot job in the map, as JSON."""
        info = {
            "name": job_name,
            "group": group_name,
            "slurm_job_id": slurm_job_id,
            "submit_time": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        self.client.map_set(
            f"{PILOT_JOB_INFO_PREFIX}{job_name}",
            json.dumps(info).encode("utf-8"),
        )

    @typechecked
    def scale_jobs(self, name: str, count: int) -> None:
        """Submit or cancel pilot jobs so the job group holds `count` of them.

        The call returns as soon as `sbatch` accepts the jobs, not when they start.
        The call raises `AssertionError`
        for a job group that `define_job_group` did not register.
        A failed `squeue` or `scancel` raises `RuntimeError`.
        A failed `sbatch` raises what `submit_sbatch_job` raises.
        """
        assert name in self.groups, "Unknown job group"

        group = self.groups[name]
        if len(group.jobs) < count:
            to_start = count - len(group.jobs)
            for _ in range(to_start):
                self._add_job(group)

        if len(group.jobs) > count:
            to_cancel = len(group.jobs) - count

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

            to_cancel_jobids: list[int] = []
            for _ in range(to_cancel):
                _, job = group.jobs.popitem()
                self.logger.info("Canceling pilot job: %s", job.name)
                if job.job_id in running_jobids:
                    to_cancel_jobids.append(job.job_id)

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

    @typechecked
    def restart_jobs(
        self, group: str, wait: bool = True, timeout: float | None = None
    ) -> int:
        """Restart the workers of a job group, and keep its pilot jobs.

        Each worker exits and a new one starts in its place,
        inside the same pilot job.
        So the job keeps its allocation and its place against its time limit.
        The new worker imports the code afresh from disk,
        and reads the actor arguments that `define_job_group` last wrote.
        The setup script does not run again,
        and the sbatch arguments do not change.
        A function defined in the driver's `__main__` travels by value
        with each task, so it needs no restart.

        A worker restarts between tasks, never during one,
        so a task in progress finishes on the code it started with.
        A pilot job that did not start yet needs no restart,
        since its workers start on the code on disk anyway.

        With `wait`, the call returns once every worker
        that ran at the time of the call exits,
        or its Slurm job ends.
        Every task claimed after that point runs on a new worker.
        The wait can take as long as the longest-running task.

        `timeout` bounds the wait, in seconds, and `None` waits without limit.
        A timeout raises `TimeoutError`.
        The restart request stays in place,
        so the remaining workers still restart after their current task.
        A worker that dies without saying so keeps the wait going
        until its Slurm job ends or the timeout expires.

        On stderr, the call reports the new workers
        that exited by the end of the wait.
        An exit that early usually means that the new code fails to start.
        Such a worker does not come back,
        so its pilot job serves the queue with fewer workers, or none.

        Without `wait`, the call returns at once,
        and a worker can claim a task or two on the old code before it restarts.

        The call returns the new restart generation of the job group.
        The call raises `AssertionError` for a job group
        that `define_job_group` did not register,
        and `ValueError` for a negative `timeout`.
        """
        assert group in self.groups, "Unknown job group"
        if timeout is not None and timeout < 0:
            raise ValueError(f"timeout must not be negative, not {timeout}")

        generation = self.client.counter_get_next_value(
            f"{RESTART_GENERATION_PREFIX}{group}"
        )
        self.logger.info(
            "Restarting the workers of %s, generation %d", group, generation
        )

        if wait:
            self._wait_for_restart(self.groups[group], generation, timeout)
        return generation

    def _wait_for_restart(
        self, group: JobGroup, generation: int, timeout: float | None
    ) -> None:
        """Block until no worker of `group` older than `generation` is live."""
        deadline = None if timeout is None else time.monotonic() + timeout

        # A worker's info never changes, and an exit is final.
        # So the loop reads each worker id once, or until it exits.
        # `old` maps a worker id to its Slurm job id.
        old: dict[str, int] = {}
        new: set[str] = set()
        done: set[str] = set()

        # None until the first `squeue` answers.
        # Until then, every job counts as live.
        live_job_ids: set[int] | None = None
        next_live_check = time.monotonic()

        while True:
            for key in self.client.map_search_key(f"^{WORKER_INFO_PREFIX}"):
                worker_id = key[len(WORKER_INFO_PREFIX) :]
                if worker_id in old or worker_id in new or worker_id in done:
                    continue
                info = json.loads(self.client.map_get(key))
                # The job name, not the worker id, since a hostname can hold dots.
                # The call does not wait on a job that this executor stopped tracking.
                if info["name"] not in group.jobs:
                    done.add(worker_id)
                elif info.get("restart_generation", 0) >= generation:
                    new.add(worker_id)
                else:
                    old[worker_id] = info["slurm_job_id"]

            for worker_id in list(old):
                if self._has_exited(worker_id):
                    del old[worker_id]
                    done.add(worker_id)

            if old and time.monotonic() >= next_live_check:
                try:
                    live_job_ids = get_running_jobids()
                except (subprocess.SubprocessError, OSError):
                    # Unknown, not dead: keep the previous answer.
                    self.logger.warning(
                        "Could not check which pilot jobs are still live; "
                        "will retry at the next interval",
                        exc_info=True,
                    )
                next_live_check = time.monotonic() + RESTART_LIVE_CHECK_INTERVAL_S
            if live_job_ids is not None:
                for worker_id, job_id in list(old.items()):
                    if job_id not in live_job_ids:
                        del old[worker_id]
                        done.add(worker_id)

            if not old:
                break

            if deadline is not None and time.monotonic() >= deadline:
                shown = ", ".join(sorted(old)[:MAX_REPORTED_WORKERS])
                if len(old) > MAX_REPORTED_WORKERS:
                    shown += f", and {len(old) - MAX_REPORTED_WORKERS} more"
                raise TimeoutError(
                    f"{len(old)} workers of job group {group.name!r} "
                    f"had not restarted after {timeout}s: {shown}. "
                    f"They still restart after their current task."
                )

            time.sleep(RESTART_POLL_INTERVAL_S)

        failed = sorted(w for w in new if self._has_exited(w))
        if failed:
            self._warn(
                f"{len(failed)} restarted workers of job group {group.name!r} "
                f"have already exited, so the new code may fail to start. "
                f"See the worker logs in {self.work_dir}: "
                f"{', '.join(failed[:MAX_REPORTED_WORKERS])}"
            )
        self.logger.info("Workers of %s restarted", group.name)

    def _has_exited(self, worker_id: str) -> bool:
        """Whether the worker `worker_id` published its exit."""
        try:
            self.client.map_get(f"{WORKER_EXIT_PREFIX}{worker_id}")
        except KeyError:
            return False
        return True

    def _submit(
        self,
        queue: list[str],
        parents: list[Task],
        priority: float,
        fn: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> Task:
        """Enqueue one task on the given queues and return its handle."""
        parent_task_ids = [parent.task_id for parent in parents]
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
            parent_task_ids=parent_task_ids,
            _parents=list(parents),
        )

        self.client.task_add(
            task_id=task_id,
            parent_task_ids=parent_task_ids,
            queue=queue,
            priority=priority,
            function=function_bytes,
            input=input_bytes,
        )
        return task

    @typechecked
    def set_task_name(self, task: Task, name: str) -> None:
        """Give `task` a name, on the server as well as locally.

        The name is for whoever reads the queue.
        Nothing here dispatches on it.
        """
        # Server first: a failed write leaves the task unnamed on both sides.
        self.client.map_set(f"task_name:{task.task_id}", name.encode("utf-8"))
        task._task_name = name

    @typechecked
    def submit(
        self,
        queue: str | list[str],
        fn: Callable | str,
        *args: Any,
        task_parents: list[Task] | None = None,
        task_priority: float = 0.0,
        **kwargs: Any,
    ) -> Task:
        """Enqueue one task and return its handle immediately.

        `fn` is a callable, or a method name for a job group with an actor.
        This method does not check the queue name.
        `wait` and `as_completed` report a task on a queue no job group serves.

        `task_parents` are the tasks this one waits on.
        The server dispatches it only after every parent finishes.
        If a parent fails, this task fails too.
        If a parent is canceled, this task is canceled too.
        The call raises `KeyError` for a parent the server does not know.

        The server dispatches the highest `task_priority` first,
        and it dispatches tasks of equal priority on one queue oldest first.
        A priority taken from a rising clock therefore serves the newest task first.
        `fn` cannot take keyword arguments
        named `task_parents` or `task_priority`,
        because this method keeps them.
        """
        if isinstance(queue, str):
            queue = [queue]

        return self._submit(
            queue, task_parents or [], task_priority, fn, *args, **kwargs
        )

    def _require_actor_queues(self, queue: list[str], what: str) -> None:
        """Refuse a method name where a job group has no actor to find it on."""
        plain = sorted(
            name
            for name in queue
            if (group := self.groups.get(name)) is not None
            and not group.actor_class_name
        )
        if plain:
            raise ValueError(
                f"{what} names a method, which only a job group with an "
                f"actor can resolve, and these groups have none: {plain}"
            )

    def _require_started_queues(self, queue: list[str], what: str) -> None:
        """Refuse a blocking call whose queues have no pilot job submitted."""
        started = {name for name, group in self.groups.items() if group.jobs}
        if not set(queue) & started:
            raise RuntimeError(
                f"{what} targets queues with no worker started: {sorted(queue)}. "
                f"Call scale_jobs() for a job group of that name first."
            )

    def _check_map_call(
        self, queue: str | list[str], map_fn: Callable | str, num_tasks: int, what: str
    ) -> list[str]:
        """Refuse what `mapreduce` and `map` refuse up front, and list the queues."""
        if num_tasks < 1:
            raise ValueError(f"num_tasks must be at least 1, not {num_tasks}")

        if isinstance(queue, str):
            queue = [queue]
        if isinstance(map_fn, str):
            self._require_actor_queues(queue, what)
        return queue

    def _enqueue_items(
        self, template: str, index: int, what: str, desc: str, items: list[Any]
    ) -> tuple[str, list[str]]:
        """Put every item on a new item queue, and return the queue and the item ids."""
        item_queue = template.format(
            name=self.name,
            index=index,
            token=uuid.uuid4().hex[:MAPREDUCE_TOKEN_LEN],
        )
        self.logger.info("%s %s: %d items on %s", what, desc, len(items), item_queue)

        item_ids: list[str] = []
        for item_index, item in enumerate(items):
            item_id = MAPREDUCE_ITEM_TEMPLATE.format(queue=item_queue, index=item_index)
            self.client.task_add(
                task_id=item_id,
                parent_task_ids=[],
                queue=[item_queue],
                # One priority for all, so the queue serves them oldest first.
                priority=0.0,
                function=NO_FUNCTION,
                input=cloudpickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL),
            )
            item_ids.append(item_id)
        return item_queue, item_ids

    def _submit_map_tasks(
        self,
        queue: list[str],
        item_queue: str,
        count: int,
        fn: Callable,
        *args: Any,
    ) -> list[Task]:
        """Submit `count` map tasks that drain `item_queue`, and name each one."""
        tasks: list[Task] = []
        for index in range(count):
            task = self._submit(queue, [], 0.0, fn, *args)
            self.set_task_name(task, f"{item_queue}.task.{index}")
            tasks.append(task)
        return tasks

    @typechecked
    def mapreduce(
        self,
        desc: str,
        queue: str | list[str],
        map_fn: Callable | str,
        reduce_fn: Callable,
        iterable: Iterable[Any],
        init: Any,
        num_tasks: int,
        map_extra_args: tuple[Any, ...] | list[Any] | None = None,
        map_extra_kwargs: dict[str, Any] | None = None,
        reduce_extra_args: tuple[Any, ...] | list[Any] | None = None,
        reduce_extra_kwargs: dict[str, Any] | None = None,
    ) -> Any:
        """Map an iterable across the pool, fold the results, and return one value.

        Tasks on `queue` claim the items,
        and each one folds what it claims into a partial result.
        This call blocks until every task is back,
        and folds the partial results into the value it returns.

        For every item it claims, a task computes:

            reduce_fn(previous, map_fn(item, *map_extra_args, **map_extra_kwargs),
                      *reduce_extra_args, **reduce_extra_kwargs)

        The first `previous` is `init`.

        `map_fn` is a callable, or the name of a method
        on the actor of the job group it runs on.
        A method name reaches the actor that worker built at startup,
        so an expensive load happens once per worker rather than once per item.
        `reduce_fn` is always a callable,
        because the fold also runs here, where there is no actor.

        Each task starts its fold at `init`, and so does this call,
        so `init` must be the identity of `reduce_fn`.
        Which items a task claims depends on how busy the pool is,
        and so does the order the partial results come back in.
        `reduce_fn` must therefore be associative and commutative.
        It must also take a partial result as its second argument
        as readily as a mapped one.
        This call folds into a copy of `init`,
        so a `reduce_fn` that folds in place
        cannot write into the caller's own value.

        `desc` labels the progress `swtop` draws for this call,
        which counts tasks rather than items.
        `num_tasks` is an upper bound.
        A call with fewer items than that submits one task per item.
        This call reads `iterable` out in full before any task starts.
        An empty one returns a copy of `init` without submitting anything.

        The call raises `ValueError` for a `num_tasks` below 1.
        The call raises it as well for a `map_fn` given as a method name
        where a job group named in `queue` has no actor to find it on.
        The call raises `RuntimeError` when no job group named in `queue`
        has a pilot job submitted.
        The call raises it as well when any of the tasks fails, as `wait` does.
        """
        queue = self._check_map_call(queue, map_fn, num_tasks, "mapreduce")

        map_args = tuple(map_extra_args or ())
        map_kwargs = dict(map_extra_kwargs or {})
        reduce_args = tuple(reduce_extra_args or ())
        reduce_kwargs = dict(reduce_extra_kwargs or {})

        # A copy, so a `reduce_fn` that folds in place
        # cannot write into the caller's `init`.
        # A cloudpickle round trip rather than `copy.deepcopy`,
        # so the local fold gets the same kind of copy each map task folds into,
        # and an `init` that cannot travel fails at the call.
        result = cloudpickle.loads(
            cloudpickle.dumps(init, protocol=pickle.HIGHEST_PROTOCOL)
        )

        # Read out in full, never streamed.
        # See Mapreduce and map in the developer notes.
        items = list(iterable)
        if not items:
            return result

        # Before anything reaches the server,
        # so a queue nobody serves costs no item task.
        self._require_started_queues(queue, "mapreduce")

        mr_queue, _ = self._enqueue_items(
            MAPREDUCE_QUEUE_TEMPLATE,
            self.next_mapreduce_index,
            "mapreduce",
            desc,
            items,
        )
        self.next_mapreduce_index += 1

        # Only after `_enqueue_items` returns.
        # See Mapreduce and map in the developer notes.
        # No more tasks than items: another one can only return `init`.
        tasks = self._submit_map_tasks(
            queue,
            mr_queue,
            min(num_tasks, len(items)),
            _mapreduce_task,
            mr_queue,
            map_fn,
            reduce_fn,
            init,
            map_args,
            map_kwargs,
            reduce_args,
            reduce_kwargs,
        )

        # Folded as they arrive,
        # so the driver never holds every partial result at once.
        for task in self.as_completed(tasks, desc=desc, unit="task"):
            result = reduce_fn(result, task.output, *reduce_args, **reduce_kwargs)
        return result

    @typechecked
    def map(
        self,
        desc: str,
        queue: str | list[str],
        map_fn: Callable | str,
        iterable: Iterable[Any],
        num_tasks: int,
        map_extra_args: tuple[Any, ...] | list[Any] | None = None,
        map_extra_kwargs: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Map an iterable across the pool, and return the values in input order.

        Tasks on `queue` claim the items, as they do for `mapreduce`,
        and each one returns the values it computed.
        This call blocks until every task is back,
        and returns a list with one value per item:

            map_fn(item, *map_extra_args, **map_extra_kwargs)

        The list is in the order of `iterable`,
        whichever task mapped an item and whenever it finished.

        `map_fn` is a callable, or the name of a method
        on the actor of the job group it runs on,
        as in `mapreduce`.

        `desc` labels the progress `swtop` draws for this call,
        which counts tasks rather than items.
        `num_tasks` is an upper bound.
        A call with fewer items than that submits one task per item.
        This call reads `iterable` out in full before any task starts.
        An empty one returns an empty list without submitting anything.

        The call raises `ValueError` for a `num_tasks` below 1.
        The call raises it as well for a `map_fn` given as a method name
        where a job group named in `queue` has no actor to find it on.
        The call raises `RuntimeError` when no job group named in `queue`
        has a pilot job submitted.
        The call raises it as well when any of the tasks fails, as `wait` does.
        The call also raises `RuntimeError` if an item comes back with no value,
        which means the ordering the call rests on broke.
        """
        queue = self._check_map_call(queue, map_fn, num_tasks, "map")

        map_args = tuple(map_extra_args or ())
        map_kwargs = dict(map_extra_kwargs or {})

        # Read out in full, never streamed.
        # See Mapreduce and map in the developer notes.
        items = list(iterable)
        if not items:
            return []

        # Before anything reaches the server,
        # so a queue nobody serves costs no item task.
        self._require_started_queues(queue, "map")

        item_queue, item_ids = self._enqueue_items(
            MAP_QUEUE_TEMPLATE, self.next_map_index, "map", desc, items
        )
        self.next_map_index += 1
        index_of = {item_id: index for index, item_id in enumerate(item_ids)}

        # Only after `_enqueue_items` returns.
        # See Mapreduce and map in the developer notes.
        # No more tasks than items: another one can only return nothing.
        tasks = self._submit_map_tasks(
            queue,
            item_queue,
            min(num_tasks, len(items)),
            _map_task,
            item_queue,
            map_fn,
            map_args,
            map_kwargs,
        )

        # A list of slots rather than a list of values,
        # since the tasks finish, and claim items, in no fixed order.
        values: list[Any] = [NoOutput] * len(items)
        for task in self.as_completed(tasks, desc=desc, unit="task"):
            for item_id, value in task.output:
                values[index_of[item_id]] = value

        # Every task came back, so an empty slot means
        # the ordering this call rests on broke.
        # See Mapreduce and map in the developer notes.
        missing = sum(1 for value in values if value is NoOutput)
        if missing:
            raise RuntimeError(
                f"map {desc}: {missing} of {len(items)} items "
                f"on {item_queue} came back with no value"
            )
        return values

    def _warn(self, message: str) -> None:
        """Report one failure on stderr as it happens."""
        print(f"warning: {message}", file=sys.stderr, flush=True)

    def _as_completed(
        self, tasks: list[Task], raise_on_error: RaiseOnError
    ) -> Iterator[Task]:
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

        # Before the first yield, so under `RAISE_ON_FIRST_ERROR`
        # a queue that never had a pilot job raises before the caller sees any result.
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
                if state == TaskState.Finished:
                    # `task_get_status` returns states only,
                    # so each finished task costs one more read.
                    output = self.client.task_get_output(task.task_id)
                    task.output = cloudpickle.loads(output)
                    completed += 1
                    yield task
                elif state == TaskState.Failed:
                    output = self.client.task_get_output(task.task_id)
                    completed += 1
                    if output.startswith(DEPENDENCY_FAILED_PREFIX):
                        # The task never ran.
                        # The server wrote this output, not a worker,
                        # so there is no traceback and no error id.
                        reason = output.decode("utf-8", errors="replace")
                        task.output = RemoteExecutionError(error=reason, error_id="")
                        failed(f"Task {task.task_id} did not run: {reason}")
                    else:
                        # The worker raised.
                        # What it produced is the failure.
                        task.output = cloudpickle.loads(output)
                        failed(
                            f"Task {task.task_id} failed on its worker: "
                            f"{task.output.error} "
                            f"(error_id={task.output.error_id})"
                        )
                    yield task
                elif state == TaskState.Canceled:
                    # Something outside this run canceled the task,
                    # or a task it waits on.
                    # The server never dispatches it again.
                    # This message and the next say "task queue server" on purpose:
                    # a user greps for that string.
                    # See docs/terminology.md, The server.
                    failed(
                        f"Task {task.task_id} was canceled on the task queue "
                        f"server, so it will never produce an output"
                    )
                elif state == TaskState.Undefined:
                    failed(f"Task {task.task_id} is unknown to the task queue server")
                else:
                    # Keep waiting.
                    # A state that falls through here waits forever,
                    # so every state a task cannot leave needs a branch above.
                    # A new state in a `ds-service` release is how this breaks.
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
    ) -> Iterator[Task]:
        """Yield each task as its output arrives.

        `desc` and `unit` label the progress `swtop` draws for this call.
        `RAISE_AFTER_COMPLETED` acts as `RAISE_ON_FIRST_ERROR` here.
        A failure raises `RuntimeError` as `raise_on_error` directs.
        A task that was canceled, is unknown to the server,
        or has no pilot job left to run it is never yielded.
        Its `output` stays `NoOutput`.
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
        The call fills in each task's `output` as `as_completed` does.
        A failure raises `RuntimeError` as `raise_on_error` directs.
        """
        tasks = list(tasks)
        progress = self._publish_progress(desc, unit, len(tasks))
        # Not through `as_completed`, which cannot defer an exception:
        # a generator has no point at which it finished but its caller did not.
        for _ in self._counted(self._as_completed(tasks, raise_on_error), progress):
            pass

    def _publish_progress(self, desc: str, unit: str, total: int) -> _Progress:
        """Announce what this call works through, and start its time series."""
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
    def _counted(tasks: Iterable[Task], progress: _Progress) -> Iterator[Task]:
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
        """Queues `squeue` still lists a job for, pending or running."""
        # A failed or timed-out `squeue` propagates,
        # and `_stranded_tasks` catches it.
        job_ids = get_running_jobids()

        groups = self.groups.values()
        if queues is not None:
            # A name no job group has is absent from the answer.
            wanted = set(queues)
            groups = [g for g in groups if g.name in wanted]

        return {
            group.name
            for group in groups
            if any(job.job_id in job_ids for job in group.jobs.values())
        }

    def _needed_queues(self, pending: list[Task]) -> dict[str, list[list[str]]]:
        """The queues each pending task and its unfinished ancestors are on."""
        # Every ancestor of every pending task, found through the handles.
        ancestors: dict[str, Task] = {}
        stack = [parent for task in pending for parent in task._parents]
        while stack:
            task = stack.pop()
            if task.task_id not in ancestors:
                ancestors[task.task_id] = task
                stack.extend(task._parents)

        # One request, and only when some task has a parent.
        # An ancestor that failed or was canceled
        # fails or cancels the task on the server,
        # and the poll loop reports that.
        # So only an unfinished ancestor still blocks.
        unfinished: set[str] = set()
        if ancestors:
            ids = list(ancestors)
            states = cast(list[TaskState], self.client.task_get_status(ids))
            blocking = (TaskState.Waiting, TaskState.Ready, TaskState.Running)
            unfinished = {i for i, state in zip(ids, states) if state in blocking}

        needed: dict[str, list[list[str]]] = {}
        for task in pending:
            queues = [task.queue]
            seen: set[str] = set()
            stack = list(task._parents)
            while stack:
                parent = stack.pop()
                if parent.task_id in seen:
                    continue
                seen.add(parent.task_id)
                if parent.task_id in unfinished:
                    queues.append(parent.queue)
                    stack.extend(parent._parents)
            needed[task.task_id] = queues
        return needed

    def _starved_tasks(self, pending: list[Task]) -> tuple[list[Task], str]:
        """Pending tasks with no pilot job ever submitted for them, and a message."""
        # This executor's own bookkeeping, not a question to the cluster:
        # the check must work before any job can start.
        started = {name for name, group in self.groups.items() if group.jobs}

        # A task also counts when an unfinished ancestor has none.
        needed = self._needed_queues(pending)
        dead = {
            task.task_id: [
                q for qs in needed[task.task_id] if not set(qs) & started for q in qs
            ]
            for task in pending
        }
        starved = [task for task in pending if dead[task.task_id]]
        if not starved:
            return [], ""

        queues = sorted({q for task in starved for q in dead[task.task_id]})
        return starved, (
            f"{len(starved)} of {len(pending)} pending tasks are on, "
            f"or wait on tasks on, queues with "
            f"no worker started: {queues}. "
            f"Call scale_jobs() for a job group of that name "
            f"before waiting on them."
        )

    def _stranded_tasks(self, pending: list[Task]) -> tuple[list[Task], str]:
        """Pending tasks whose queues have no pilot job left, and a message."""
        # A task also counts when an unfinished ancestor's queues have none.
        needed = self._needed_queues(pending)
        try:
            live = self._live_queues(
                {q for queues in needed.values() for qs in queues for q in qs}
            )
        except (subprocess.SubprocessError, OSError):
            # An unreachable `squeue` leaves liveness unknown, not dead,
            # so this gives up no task, and the next interval retries.
            self.logger.warning(
                "Could not check whether queues are still live; "
                "will retry at the next interval",
                exc_info=True,
            )
            return [], ""

        dead = {
            task.task_id: [
                q for qs in needed[task.task_id] if not set(qs) & live for q in qs
            ]
            for task in pending
        }
        stranded = [task for task in pending if dead[task.task_id]]
        if not stranded:
            return [], ""

        queues = sorted({q for task in stranded for q in dead[task.task_id]})
        return stranded, (
            f"{len(stranded)} of {len(pending)} pending tasks are on, "
            f"or wait on tasks on, queues with "
            f"no live pilot job, so they can never run: {queues}. "
            f"Scale up a job group named after one of those queues, "
            f"or cancel the wait."
        )

    def _cleanup_all_workers(self) -> None:
        """Cancel every live pilot job, and report a failure rather than raise it."""
        # `close()` must still close the client and the log after a failure here,
        # so this method reports each failure and drops it.
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

        to_cancel_jobids: list[int] = []
        for group in self.groups.values():
            for job in group.jobs.values():
                if job.job_id in job_ids:
                    to_cancel_jobids.append(job.job_id)

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
        """Detach and close this executor's log handler, if it is still attached."""
        handler = self._log_handler
        if handler is None:
            return

        # Cleared first, so a later failure cannot double-close it.
        self._log_handler = None
        self.logger.removeHandler(handler)
        handler.close()

    def close(self) -> None:
        """Cancel every pilot job and close the connection to the server.

        The executor is spent afterward.
        Python calls this at the end of a `with` block.
        The call prints or logs a failure to list or cancel the jobs,
        and does not raise it.
        """
        self._cleanup_all_workers()
        for group in self.groups.values():
            group.jobs.clear()

        self.client.close()
        self._close_log_handler()

    def stop(self) -> None:
        """Cancel every pilot job, and leave the executor usable.

        The call prints or logs a failure to list or cancel the jobs,
        and does not raise it.
        The executor forgets the jobs either way.
        """
        self._cleanup_all_workers()
        for group in self.groups.values():
            group.jobs.clear()

    def __enter__(self) -> "SlurmPilotExecutor":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
