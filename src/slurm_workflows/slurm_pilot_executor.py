"""The executor: job groups, pilot jobs, and the tasks they run.

One executor per `ds-service` server.
`docs/explanation/pilot-job-model.md` explains the model.
"""

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

from .slurm_pilot_worker import current_actor

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
# The series under the id it carries holds the count completed so far.
PROGRESS_DISPLAY_KEY = "progress_display"
PROGRESS_SERIES_PREFIX = "progress:"

# How often the executor appends the count while tasks come back.
PROGRESS_INTERVAL_S: float = 1.0

# The queue one `mapreduce` call puts its items on,
# and the id of each item task on that queue.
# The `mapreduce` segment keeps these ids clear of `<name>.task.<n>`.
# The token keeps two runs of one executor name clear of each other:
# the index restarts at 0 in each executor,
# and `task_add` refuses a duplicate id.
MAPREDUCE_QUEUE_TEMPLATE = "{name}.mapreduce.{index}.{token}"
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

# How ds-service begins the output of a task it failed
# because a task it waits on failed.
# The rest of that output names the failed task.
# The output is plain text, not a cloudpickle.
DEPENDENCY_FAILED_PREFIX = b"Dependency failed"


class RaiseOnError(Enum):
    """What `as_completed` and `wait` do about a task that fails.

    A failure is a task whose worker raised,
    one canceled on the server,
    one whose parent task failed or was canceled,
    or one the server does not know.
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

# What you can name an executor:
# the intersection of what is safe in a task id, a logger name,
# a directory name and a Slurm job name.
EXECUTOR_NAME_REGEX = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
MIN_EXECUTOR_NAME_LEN: int = 3

# How long `_as_completed` sleeps between two status polls
# that brought back nothing.
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
    # and an assignment would rename the task in this process alone.
    @property
    def task_name(self) -> str | None:
        """The name `SlurmPilotExecutor.set_task_name` set, or None."""
        return self._task_name


@dataclass
class JobGroup:
    """A named recipe for pilot jobs, and the queue their workers serve.

    `scale_jobs` sets how many pilot jobs the job group has.
    How many workers those jobs start is decided by the sbatch arguments
    and `is_batch_worker`.
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
        """Append `done`, unless the series took a value recently."""
        now = time.monotonic()
        # The final count always goes out, whatever the interval.
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


def _resolve_map_method(name: str) -> Callable:
    """Look one map method name up on the actor of the worker running it."""
    actor = current_actor()
    if actor is None:
        raise RuntimeError(
            f"mapreduce map_fn names the method {name!r}, "
            f"but the worker running this task has no actor"
        )
    return getattr(actor, name)


def _mapreduce_task(
    mr_queue: str,
    map_fn: Callable | str,
    reduce_fn: Callable,
    init: Any,
    map_args: tuple,
    map_kwargs: dict,
    reduce_args: tuple,
    reduce_kwargs: dict,
) -> Any:
    """Map and fold every item this task claims from `mr_queue`."""
    # The id of the worker that runs this task, from the environment.
    # One worker runs one task at a time,
    # so its id names this map task as well.
    worker_id = os.environ["PILOT_WORKER_ID"]

    # Once, not once per item.
    # See Mapreduce in the developer notes.
    if isinstance(map_fn, str):
        map_fn = _resolve_map_method(map_fn)

    result = init

    # A client of this task's own.
    # `DsServiceClient()` reads the address the worker put in the environment.
    # See Mapreduce in the developer notes.
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


class SlurmPilotExecutor:
    """Runs Python callables on a Slurm cluster through a pool of workers.

    Each executor needs a `ds-service` server of its own,
    because everything on a server belongs to one run.
    The executor cancels its pilot jobs in `close()` and `stop()`,
    and `scale_jobs` cancels those above the count it is given.
    A `with` block calls `close()` at its end.
    """

    @typechecked
    def __init__(
        self,
        name: str,
        server_address: str,
        work_dir: Path | str | None = None,
    ) -> None:
        """Connect to a `ds-service` server and open a work directory for this run.

        `name` prefixes task ids and pilot job names, and names the logger,
        so two executors on one cluster need two names.
        `name` must match `[A-Za-z][A-Za-z0-9_-]*`
        and hold at least `MIN_EXECUTOR_NAME_LEN` characters.
        Anything else raises `ValueError`.
        `work_dir` defaults to a timestamped directory in the user cache.
        The executor creates the work directory if it does not exist,
        prints its path on stdout, and logs to `executor.log` in it.
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
        self.next_mapreduce_index = 0

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
        `python_paths` go on the front of each worker's `sys.path`.
        `add_cwd_to_python_path` adds the driver's current directory
        as it is at this call.
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

        Returns as soon as `sbatch` accepts the jobs, not when they start.
        Raises `AssertionError` for a job group `define_job_group` did not register.
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
        # 0.0 for every task keeps submission order,
        # since equal priorities are served oldest first.
        # A priority taken from a rising clock would serve the newest task first.
        task_priority: float = 0.0,
        **kwargs: Any,
    ) -> Task:
        """Enqueue one task and return its handle immediately.

        `fn` is a callable, or a method name for a job group with an actor.
        This method does not check the queue name.
        `wait` and `as_completed` report a task on a queue no job group serves.

        `task_parents` are the tasks this one waits on.
        The server dispatches it only after every parent finishes.
        A parent that fails fails this task too,
        and a parent that is canceled cancels it.
        Raises `KeyError` for a parent the server does not know.

        The server dispatches the highest `task_priority` first,
        and tasks of equal priority on one queue oldest first.
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

        The call puts every item of `iterable` on a queue of its own.
        Tasks on `queue` drain that queue,
        and the call blocks until all of them are back.
        Each of those tasks folds what it claims into a partial result,
        and this call folds the partial results into the value it returns.

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
        `iterable` is read out in full before any task starts,
        and an empty one returns a copy of `init` without submitting anything.

        Raises `ValueError` for a `num_tasks` below 1.
        Raises it as well for a `map_fn` given as a method name
        where a job group named in `queue` has no actor to find it on.
        Raises `RuntimeError` when no job group named in `queue`
        has a pilot job submitted,
        and when any of the tasks fails, as `wait` does.
        """
        if num_tasks < 1:
            raise ValueError(f"num_tasks must be at least 1, not {num_tasks}")

        if isinstance(queue, str):
            queue = [queue]
        if isinstance(map_fn, str):
            self._require_actor_queues(queue, "mapreduce")

        map_args = tuple(map_extra_args or ())
        map_kwargs = dict(map_extra_kwargs or {})
        reduce_args = tuple(reduce_extra_args or ())
        reduce_kwargs = dict(reduce_extra_kwargs or {})

        # A cloudpickle round trip, like the copy each map task folds into.
        # See Mapreduce in the developer notes.
        result = cloudpickle.loads(
            cloudpickle.dumps(init, protocol=pickle.HIGHEST_PROTOCOL)
        )

        # Read out in full, never streamed.
        # See Mapreduce in the developer notes.
        items = list(iterable)
        if not items:
            return result

        # Before anything reaches the server,
        # so a queue nobody serves costs no item task.
        self._require_started_queues(queue, "mapreduce")

        mr_queue = MAPREDUCE_QUEUE_TEMPLATE.format(
            name=self.name,
            index=self.next_mapreduce_index,
            token=uuid.uuid4().hex[:MAPREDUCE_TOKEN_LEN],
        )
        self.next_mapreduce_index += 1
        self.logger.info("mapreduce %s: %d items on %s", desc, len(items), mr_queue)

        for index, item in enumerate(items):
            self.client.task_add(
                task_id=MAPREDUCE_ITEM_TEMPLATE.format(queue=mr_queue, index=index),
                parent_task_ids=[],
                queue=[mr_queue],
                # One priority for all, so the queue serves them oldest first.
                priority=0.0,
                function=NO_FUNCTION,
                input=cloudpickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL),
            )

        # Every item is on the queue by now,
        # which is what lets a task read an empty queue as a finished one.
        # No more tasks than items: another one can only return `init`.
        tasks: list[Task] = []
        for index in range(min(num_tasks, len(items))):
            task = self._submit(
                queue,
                [],
                0.0,
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
            self.set_task_name(task, f"{mr_queue}.task.{index}")
            tasks.append(task)

        # Folded as they arrive,
        # so the driver never holds every partial result at once.
        for task in self.as_completed(tasks, desc=desc, unit="task"):
            result = reduce_fn(result, task.output, *reduce_args, **reduce_kwargs)
        return result

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
        """Yield tasks as their results arrive.

        `desc` and `unit` label the progress `swtop` draws for this call.
        `RAISE_AFTER_COMPLETED` acts as `RAISE_ON_FIRST_ERROR` here.
        A failure raises `RuntimeError` as `raise_on_error` directs.
        A task that was canceled, is unknown to the server,
        or has no pilot job left to run it is never yielded,
        and its `output` stays `NoOutput`.
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
        It fills in each task's `output` as `as_completed` does.
        A failure raises `RuntimeError` as `raise_on_error` directs.
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
        # and the poll loop reports that, so only these still block.
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
            # Unreachable `squeue` leaves liveness unknown, not dead,
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
        # `close()` still has to close the client and the log after a failure here,
        # so each failure is reported and dropped.
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
        A failure to list or cancel the jobs is printed or logged, not raised.
        """
        self._cleanup_all_workers()
        for group in self.groups.values():
            group.jobs.clear()

        self.client.close()
        self._close_log_handler()

    def stop(self) -> None:
        """Cancel every pilot job, and leave the executor usable.

        A failure to list or cancel the jobs is printed or logged, not raised,
        and the executor forgets the jobs either way.
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
