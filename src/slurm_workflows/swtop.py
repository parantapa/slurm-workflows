"""`swtop`: a live view of a `ds-service` task queue.

See `docs/reference/swtop.md` for the blocks and what fills them.
"""

# This module decides what to show.
# `swtop_widgets.py` draws it, and `swtop_tui.py` lays those widgets out.
# "Monitoring" in `docs/developer-notes.md` says
# why `swtop` collects the way it does.

from __future__ import annotations

import sys
import json
import asyncio
from typing import Callable, cast, AsyncIterator
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field, replace
from contextlib import asynccontextmanager

import click
from ds_service_client import DsServiceClientAsync, TaskState, TaskStateError

from .monitors import HOST_SERIES, JOB_SERIES, split_job_subject
from .slurm_pilot_executor import (
    PROGRESS_DISPLAY_KEY,
    PROGRESS_SERIES_PREFIX,
    PILOT_JOB_INFO_PREFIX,
)
from .slurm_pilot_worker import (
    WORKER_INFO_PREFIX,
    WORKER_EXIT_PREFIX,
    PILOT_JOB_START_PREFIX,
    PILOT_JOB_EXIT_PREFIX,
)

DEFAULT_INTERVAL_S: float = 2.0

# The fields a worker publishes about itself,
# in the order it writes them.
# The tables order their own columns.
WORKER_INFO_FIELDS = ["group", "name", "slurm_job_id", "hostname", "pid", "start_time"]

# The fields the executor publishes about a pilot job, likewise.
PILOT_JOB_FIELDS = ["name", "group", "slurm_job_id", "submit_time"]

# The fields of the progress display a wait publishes.
PROGRESS_FIELDS = ["progress_id", "desc", "unit", "total"]

# How far back each poll reads the progress series.
# With nothing that recent, the display keeps the last count this collector saw.
PROGRESS_TAIL_S = 60.0

# Must match the key `SlurmPilotExecutor.set_task_name` writes,
# which spells it out rather than importing a constant.
TASK_NAME_PREFIX = "task_name:"

# `task_search_id` matches task ids against a regular expression,
# and an empty one matches every task the server holds.
ALL_TASK_IDS = ""

# The tables show this in place of the name of a task that nothing named.
UNNAMED = "-"

# The pilot jobs table shows this in place of the start time of a queued job.
NOT_STARTED = "-"

# How far back a monitored value is still worth showing.
# Twelve readings at the default `DEFAULT_MONITOR_INTERVAL_S` of 5 seconds.
STALE_AFTER_S = 60.0

# The tables show this in place of a value the collector cannot read:
# a field of a worker or a pilot job whose key is in the map,
# or the name of a task.
# A value that is not the JSON its writer publishes looks like this.
UNKNOWN = "?"

# The order the tables list tasks in: what runs now comes first.
STATE_ORDER = [
    "Running",
    "Ready",
    "Waiting",
    "Failed",
    "Finished",
    "Canceled",
    "Undefined",
]

# What each block says when it has nothing to show.
# Each says why it is empty, since an empty block is usually a question.
EMPTY_PILOT_JOBS = (
    "no pilot jobs have been submitted through this server, or all of them exited"
)
EMPTY_WORKERS = "no workers have registered with this server, or all of them exited"
EMPTY_HOSTS = "no host is being monitored"
EMPTY_JOBS = "no slurm job is being monitored"
EMPTY_TASKS = "no tasks have been submitted to this server"


@dataclass
class WorkerInfo:
    """One registered worker, as it describes itself in the map."""

    worker_id: str
    group: str
    name: str
    slurm_job_id: str
    hostname: str
    pid: str
    start_time: str = UNKNOWN


@dataclass
class PilotJobInfo:
    """One pilot job, as the executor described it when it submitted it.

    `start_time` is what the pilot job published when it started,
    or `NOT_STARTED` for a job that has not published one.
    """

    name: str
    group: str
    slurm_job_id: str
    submit_time: str
    start_time: str = NOT_STARTED


@dataclass
class TaskInfo:
    """One task, with the state the server reports for it.

    `name` is what `SlurmPilotExecutor.set_task_name` published for it,
    or `UNNAMED` for a task nothing named.
    """

    task_id: str
    name: str
    state: str
    worker: str = ""


@dataclass
class SubjectInfo:
    """The latest reading of one monitored host, or of one job on one node.

    `values` is empty when the subject's series exist but hold nothing recent.
    A dead monitor looks like this.
    """

    subject: str
    values: dict[str, float] = field(default_factory=dict)

    @property
    def stale(self) -> bool:
        """Whether the collector read nothing recent, which is a dead monitor."""
        return not self.values


@dataclass
class ProgressInfo:
    """What a `wait` or `as_completed` call works through."""

    progress_id: str
    desc: str
    unit: str
    total: int
    completed: int = 0

    @property
    def done(self) -> bool:
        """Whether the wait this describes finished."""
        return self.completed >= self.total

    @property
    def fraction(self) -> float:
        """How far along the wait is, in `[0, 1]`.

        An empty wait counts as done.
        """
        return self.completed / self.total if self.total else 1.0


@dataclass
class Snapshot:
    """One poll's worth of server state.

    `jobs` holds one entry per Slurm job per node it runs on.
    `worker_jobs`, `workers` and `jobs` leave out
    the pilot jobs and the workers that published their exit,
    and the Slurm jobs of those pilot jobs.
    A poll that failed sets `error` and leaves every other reading empty.
    """

    address: str
    when: datetime
    counts: dict[str, int] = field(default_factory=dict)
    progress: ProgressInfo | None = None
    worker_jobs: list[PilotJobInfo] = field(default_factory=list)
    workers: list[WorkerInfo] = field(default_factory=list)
    tasks: list[TaskInfo] = field(default_factory=list)
    hosts: list[SubjectInfo] = field(default_factory=list)
    jobs: list[SubjectInfo] = field(default_factory=list)
    error: str | None = None


class Collector:
    """Turns the server's RPCs into a `Snapshot`.

    One instance is meant to serve every poll of one server.
    It reads each worker, pilot job and task name once, and caches them.
    """

    def __init__(self, client: DsServiceClientAsync, address: str) -> None:
        self.client = client
        self.address = address
        # See "`Collector` reads an identity once" in the developer notes.
        self._pilot_jobs: dict[str, PilotJobInfo] = {}
        self._pilot_job_starts: dict[str, str] = {}
        self._workers: dict[str, WorkerInfo] = {}
        # The last count read for a progress id,
        # so a display that no longer moves still shows where it stopped.
        self._progress_seen: dict[str, int] = {}
        # Cached like the identities above.
        self._task_names: dict[str, str] = {}

    async def snapshot(self) -> Snapshot:
        """One poll of the server, as a `Snapshot`.

        A key the server does not hold does not raise.
        A missing worker or pilot job description reads as `UNKNOWN`,
        a missing progress display as no progress,
        and a running task with no known worker as an empty worker.
        But a server that the client cannot reach raises.
        The caller decides whether to keep polling.
        """
        # None of these needs an answer from another,
        # so they go out together and the poll waits once.
        # Two groups, since `asyncio.gather` types at most six at a time.
        (counts, progress, worker_jobs, workers, hosts, jobs), exited = (
            await asyncio.gather(
                asyncio.gather(
                    self.client.task_get_count_by_state(),
                    self._collect_progress(),
                    self._collect_pilot_jobs(),
                    self._collect_workers(),
                    self._collect_subjects(HOST_SERIES),
                    self._collect_subjects(JOB_SERIES),
                ),
                asyncio.gather(
                    self._exited(PILOT_JOB_EXIT_PREFIX),
                    self._exited(WORKER_EXIT_PREFIX),
                ),
            )
        )
        exited_jobs, exited_workers = exited
        # The tasks do need the workers:
        # a running task carries the name of the worker that holds it.
        # Every worker, since one that just exited can still hold a task.
        tasks = await self._collect_tasks(workers)

        exited_slurm_jobs = {
            j.slurm_job_id for j in worker_jobs if j.name in exited_jobs
        }

        return Snapshot(
            address=self.address,
            when=datetime.now(),
            counts={
                "waiting": counts.waiting,
                "ready": counts.ready,
                "running": counts.running,
                "finished": counts.finished,
                "failed": counts.failed,
                "canceled": counts.canceled,
            },
            progress=progress,
            worker_jobs=[j for j in worker_jobs if j.name not in exited_jobs],
            workers=[w for w in workers if w.worker_id not in exited_workers],
            tasks=tasks,
            hosts=hosts,
            jobs=[
                j
                for j in jobs
                if split_job_subject(j.subject)[0] not in exited_slurm_jobs
            ],
        )

    async def _exited(self, prefix: str) -> set[str]:
        """The names under `prefix` that published their exit."""
        # The key alone says so, and nothing reads the time in it.
        return {
            key[len(prefix) :] for key in await self.client.map_search_key(f"^{prefix}")
        }

    async def _text(self, key: str) -> str:
        """One key's value as text, or `UNKNOWN` if the server does not hold it."""
        try:
            value = await self.client.map_get(key)
        except KeyError:
            return UNKNOWN
        return value.decode("utf-8", errors="replace")

    async def _collect_progress(self) -> ProgressInfo | None:
        """The progress display a wait published, and how far it got."""
        text = await self._text(PROGRESS_DISPLAY_KEY)
        # A server with no display reads as `UNKNOWN`, which is not JSON.
        try:
            published = json.loads(text)
            info = ProgressInfo(
                progress_id=str(published["progress_id"]),
                desc=str(published["desc"]),
                unit=str(published["unit"]),
                total=int(published["total"]),
            )
        except (ValueError, TypeError, KeyError):
            return None

        since = (
            datetime.now(timezone.utc) - timedelta(seconds=PROGRESS_TAIL_S)
        ).isoformat()
        points = await self.client.time_series_get(
            f"{PROGRESS_SERIES_PREFIX}{info.progress_id}", start_time=since
        )
        if points:
            self._progress_seen[info.progress_id] = int(points[-1].value)
        info.completed = self._progress_seen.get(info.progress_id, 0)
        return info

    async def _collect_pilot_jobs(self) -> list[PilotJobInfo]:
        """Every pilot job the executor published, with when it started."""
        info_keys, start_keys = await asyncio.gather(
            self.client.map_search_key(f"^{PILOT_JOB_INFO_PREFIX}"),
            self.client.map_search_key(f"^{PILOT_JOB_START_PREFIX}"),
        )
        names = [key[len(PILOT_JOB_INFO_PREFIX) :] for key in info_keys]
        started = {key[len(PILOT_JOB_START_PREFIX) :] for key in start_keys}

        missing = [n for n in names if n not in self._pilot_jobs]
        missing_starts = sorted(started - self._pilot_job_starts.keys())
        await asyncio.gather(
            *(self._pilot_job_info(n) for n in missing),
            *(self._pilot_job_start(n) for n in missing_starts),
        )

        listed = [
            replace(
                self._pilot_jobs.get(name) or _unknown_pilot_job(name),
                start_time=self._pilot_job_starts.get(name, NOT_STARTED),
            )
            for name in names
        ]
        return sorted(listed, key=lambda j: (j.group, j.name))

    async def _pilot_job_start(self, name: str) -> None:
        """Cache when one job started, if its key is readable."""
        text = await self._text(f"{PILOT_JOB_START_PREFIX}{name}")
        try:
            start_time = str(json.loads(text)["start_time"])
        except (ValueError, TypeError, KeyError):
            return
        self._pilot_job_starts[name] = start_time

    async def _pilot_job_info(self, name: str) -> None:
        """Cache one job's published description, if it is readable."""
        text = await self._text(f"{PILOT_JOB_INFO_PREFIX}{name}")
        try:
            published = json.loads(text)
            fields = {field: str(published[field]) for field in PILOT_JOB_FIELDS}
        except (ValueError, TypeError, KeyError):
            return

        # See "`Collector` reads an identity once" in the developer notes.
        self._pilot_jobs[name] = PilotJobInfo(**fields)

    async def _collect_workers(self) -> list[WorkerInfo]:
        """Every worker that registered, cached like the rest."""
        worker_ids = [
            key[len(WORKER_INFO_PREFIX) :]
            for key in await self.client.map_search_key(f"^{WORKER_INFO_PREFIX}")
        ]

        # Every description the cache is short of, read in one go.
        missing = [w for w in worker_ids if w not in self._workers]
        await asyncio.gather(*(self._worker_info(w) for w in missing))

        listed = [
            self._workers.get(worker_id) or _unknown_worker(worker_id)
            for worker_id in worker_ids
        ]
        return sorted(listed, key=lambda w: (w.group, w.name))

    async def _worker_info(self, worker_id: str) -> None:
        """Cache one worker's published description, if it is readable."""
        text = await self._text(f"{WORKER_INFO_PREFIX}{worker_id}")
        try:
            published = json.loads(text)
            fields = {name: str(published[name]) for name in WORKER_INFO_FIELDS}
        except (ValueError, TypeError, KeyError):
            # An unreadable description does not go in the cache,
            # so the next poll reads the key again.
            return

        self._workers[worker_id] = WorkerInfo(worker_id=worker_id, **fields)

    async def _collect_tasks(self, workers: list[WorkerInfo]) -> list[TaskInfo]:
        """Every task on the server, in the order the tasks block lists them."""
        # Which tasks there are, and which of them have a name:
        # two searches, neither of which needs the other's answer.
        task_ids, name_keys = await asyncio.gather(
            self.client.task_search_id(ALL_TASK_IDS),
            self.client.map_search_key(f"^{TASK_NAME_PREFIX}"),
        )
        if not task_ids:
            return []

        # Only names already written go in the cache.
        # See "`Collector` reads an identity once" in the developer notes.
        named = {key[len(TASK_NAME_PREFIX) :] for key in name_keys}
        missing = sorted(named - self._task_names.keys())
        await asyncio.gather(*(self._task_name(task_id) for task_id in missing))

        # One batched call for every task,
        # rather than a status RPC apiece.
        states = await self.client.task_get_status(task_ids)
        # A list of ids answers with a list of states, one per id.
        states = cast(list[TaskState], states)
        worker_names = {w.worker_id: w.name for w in workers}

        tasks = [
            TaskInfo(
                task_id=task_id,
                name=self._task_names.get(task_id, UNNAMED),
                state=TaskState(state).name,
            )
            for task_id, state in zip(task_ids, states)
        ]

        # Who holds a task is a read apiece, so the running ones go together.
        running = [t for t in tasks if t.state == "Running"]
        holders = await asyncio.gather(
            *(self._holder(t.task_id, worker_names) for t in running)
        )
        for task, holder in zip(running, holders):
            task.worker = holder

        # Named before unnamed within a state:
        # a name marks a task somebody wanted to find again.
        return sorted(
            tasks,
            key=lambda t: (_state_rank(t.state), t.name == UNNAMED, t.name, t.task_id),
        )

    async def _task_name(self, task_id: str) -> None:
        """Cache one task's name."""
        self._task_names[task_id] = await self._text(f"{TASK_NAME_PREFIX}{task_id}")

    async def _collect_subjects(self, prefixes: dict[str, str]) -> list[SubjectInfo]:
        """The latest reading of every subject one monitor writes about."""
        # The keys of one series name every subject.
        first = next(iter(prefixes.values()))
        subjects = sorted(
            key[len(first) :]
            for key in await self.client.time_series_search_key(f"^{first}")
        )
        if not subjects:
            return []

        # The tail only: with no bounds, `time_series_get` returns every point,
        # which over a day-long run is most of the memory the server holds.
        since = (
            datetime.now(timezone.utc) - timedelta(seconds=STALE_AFTER_S)
        ).isoformat()

        # Every series of every subject in one go.
        wanted = [
            (subject, name, prefix)
            for subject in subjects
            for name, prefix in prefixes.items()
        ]
        series = await asyncio.gather(
            *(
                self.client.time_series_get(f"{prefix}{subject}", start_time=since)
                for subject, _, prefix in wanted
            )
        )

        readings = {subject: SubjectInfo(subject=subject) for subject in subjects}
        for (subject, name, _), points in zip(wanted, series):
            if points:
                readings[subject].values[name] = points[-1].value
        return [readings[subject] for subject in subjects]

    async def _holder(self, task_id: str, worker_names: dict[str, str]) -> str:
        """The worker that runs `task_id`, by name, or "" if the server cannot say."""
        try:
            worker_id = await self.client.task_get_worker_id(task_id)
        except (KeyError, TaskStateError):
            # The task can leave Running between the status call and this read.
            return ""
        return worker_names.get(worker_id, worker_id)


@asynccontextmanager
async def open_collector(address: str) -> AsyncIterator[Collector]:
    """A collector on a client of its own, closed on the way out.

    The client belongs to the event loop that enters this,
    so use the collector only on that loop.
    """
    async with DsServiceClientAsync(address) as client:
        yield Collector(client, address)


def _unknown_worker(worker_id: str) -> WorkerInfo:
    """A row for a worker whose description the collector cannot read."""
    return WorkerInfo(
        worker_id=worker_id, **{name: UNKNOWN for name in WORKER_INFO_FIELDS}
    )


def _unknown_pilot_job(name: str) -> PilotJobInfo:
    """A row for a pilot job whose description the collector cannot read."""
    return PilotJobInfo(
        **{field: UNKNOWN for field in PILOT_JOB_FIELDS} | {"name": name}
    )


def _state_rank(state: str) -> int:
    """Where a state sorts, with a state `STATE_ORDER` omits sorting last."""
    try:
        return STATE_ORDER.index(state)
    except ValueError:
        return len(STATE_ORDER)


def _bytes(value: float) -> str:
    """Bytes as the largest unit that keeps the number readable."""
    for unit in ["B", "K", "M", "G", "T"]:
        if abs(value) < 1024 or unit == "T":
            return f"{value:.1f}{unit}"
        value /= 1024
    # Unreachable, since the loop returns at "T".
    # It is here for the type checker.
    return f"{value:.1f}T"


def _subject(info: SubjectInfo) -> str:
    """The subject's name, marked `(stale)` where its series stopped."""
    return info.subject if not info.stale else f"{info.subject} (stale)"


def _cell(values: dict[str, float], name: str, fmt: Callable[[float], str]) -> str:
    """One measurement, or a dash where the series had nothing recent."""
    if name not in values:
        return "-"
    return fmt(values[name])


# The columns of each block, which the text frames and the UI share.
PILOT_JOB_COLUMNS = ["NAME", "GROUP", "JOB", "SUBMITTED", "STARTED"]
WORKER_COLUMNS = ["NAME", "GROUP", "HOST", "JOB", "PID", "STARTED"]
HOST_COLUMNS = ["HOST", "FREE MEM", "LOAD", "/dev/shm", "/tmp"]
JOB_COLUMNS = ["JOB", "HOST", "MEMORY", "CPU"]
TASK_COLUMNS = ["NAME", "TASK ID", "STATE", "WORKER"]


def progress_line(snapshot: Snapshot, width: int = 24) -> str:
    """The progress display as one line, or "" when nothing published one.

    `width` is the width of the bar in characters, not of the whole line.
    """
    progress = snapshot.progress
    if progress is None:
        return ""

    filled = round(progress.fraction * width)
    bar = "#" * filled + "-" * (width - filled)
    state = "done" if progress.done else "working"
    return (
        f"{progress.desc}  [{bar}]  "
        f"{progress.completed}/{progress.total} {progress.unit}  "
        f"{progress.fraction:.0%}  {state}"
    )


def counts_line(snapshot: Snapshot) -> str:
    """The task counts, as one line."""
    counts = snapshot.counts
    total = sum(counts.values())
    return (
        "tasks  "
        + "  ".join(f"{name} {counts[name]}" for name in counts)
        + f"  total {total}"
    )


def pilot_job_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per pilot job that has not exited, keyed by its job name."""
    return [
        (
            job.name,
            [job.name, job.group, job.slurm_job_id, job.submit_time, job.start_time],
        )
        for job in snapshot.worker_jobs
    ]


def worker_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per registered worker that has not exited, keyed by its worker id."""
    return [
        (
            w.worker_id,
            [w.name, w.group, w.hostname, w.slurm_job_id, w.pid, w.start_time],
        )
        for w in snapshot.workers
    ]


def host_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per monitored host, keyed by its hostname."""
    return [
        (
            host.subject,
            [
                _subject(host),
                _cell(host.values, "free_memory", _bytes),
                _cell(host.values, "load_average", lambda v: f"{v:.2f}"),
                _cell(host.values, "dev_shm_used", lambda v: f"{v:.1f}%"),
                _cell(host.values, "tmp_used", lambda v: f"{v:.1f}%"),
            ],
        )
        for host in snapshot.hosts
    ]


def job_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per monitored Slurm job per node, keyed by `<job-id>:<hostname>`."""
    rows = []
    for job in snapshot.jobs:
        slurm_job_id, hostname = split_job_subject(job.subject)
        if job.stale:
            slurm_job_id = f"{slurm_job_id} (stale)"
        rows.append(
            (
                job.subject,
                [
                    slurm_job_id,
                    # A subject with no hostname was written by a worker
                    # from before the job series were per node.
                    hostname or UNKNOWN,
                    _cell(job.values, "memory", _bytes),
                    _cell(job.values, "cpu", lambda v: f"{v:.1f} cores"),
                ],
            )
        )
    return rows


def task_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per task, keyed by its task id."""
    return [
        (task.task_id, [task.name, task.task_id, task.state, task.worker])
        for task in snapshot.tasks
    ]


@dataclass(frozen=True)
class BlockSpec:
    """What one block shows, for both displays."""

    # Names the block in ids, such as the `swtop-workers` tab.
    key: str
    title: str
    columns: list[str]
    empty: str
    rows: Callable[[Snapshot], list[tuple[str, list[str]]]]


# The blocks, in the order both displays show them.
BLOCKS: tuple[BlockSpec, ...] = (
    BlockSpec(
        "pilot-jobs", "pilot jobs", PILOT_JOB_COLUMNS, EMPTY_PILOT_JOBS, pilot_job_rows
    ),
    BlockSpec("workers", "workers", WORKER_COLUMNS, EMPTY_WORKERS, worker_rows),
    BlockSpec("hosts", "hosts", HOST_COLUMNS, EMPTY_HOSTS, host_rows),
    BlockSpec("jobs", "slurm jobs", JOB_COLUMNS, EMPTY_JOBS, job_rows),
    BlockSpec("tasks", "tasks", TASK_COLUMNS, EMPTY_TASKS, task_rows),
)


def block_label(spec: BlockSpec, count: int) -> str:
    """The title of a block with its row count, such as `workers (40)`."""
    return f"{spec.title} ({count})"


def _table(headers: list[str], rows: list[list[str]]) -> list[str]:
    """Left-aligned fixed-width columns, sized to their contents."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    for row in rows:
        lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip())
    return lines


def render(snapshot: Snapshot) -> str:
    """The whole screen, as text."""
    when = snapshot.when.strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"swtop  {snapshot.address}  {when}", ""]

    if snapshot.error is not None:
        lines.append(f"cannot read the server: {snapshot.error}")
        return "\n".join(lines) + "\n"

    lines.append(counts_line(snapshot))
    lines.append("")

    progress = progress_line(snapshot)
    if progress:
        lines.append(progress)
        lines.append("")

    for spec in BLOCKS:
        rows = spec.rows(snapshot)
        lines.append(block_label(spec, len(rows)))
        if rows:
            lines.extend(_table(spec.columns, [cells for _, cells in rows]))
        else:
            lines.append(spec.empty)
        lines.append("")

    return "\n".join(lines[:-1]) + "\n"


def draw(text: str) -> None:
    """Put `text` on the screen, in place of what was there.

    Append it instead, without escape codes, when stdout is not a terminal.
    """
    if sys.stdout.isatty():
        # Home, then clear.
        # The other order leaves the old frame visible for a moment on a slow link.
        sys.stdout.write("\x1b[H\x1b[2J")
    sys.stdout.write(text)
    if not sys.stdout.isatty():
        sys.stdout.write("\n")
    sys.stdout.flush()


async def run_plain(collector: Collector, interval: float) -> None:
    """Poll and print frames until the viewer interrupts."""
    try:
        while True:
            try:
                snapshot = await collector.snapshot()
            except Exception as e:
                # See "`swtop` draws a failed poll" in the developer notes.
                snapshot = Snapshot(
                    address=collector.address,
                    when=datetime.now(),
                    error=f"{type(e).__name__}: {e}",
                )

            draw(render(snapshot))
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        # Ctrl-C is the way this ends.
        pass


async def watch(server_address: str, interval: float, plain: bool) -> None:
    """Open a client on the running loop, and run a display on it.

    The display is the text frames when `plain` is set or stdout is not a terminal,
    and the terminal UI otherwise.
    """
    async with open_collector(server_address) as collector:
        if plain or not sys.stdout.isatty():
            await run_plain(collector, interval)
        else:
            # The import sits here, so the text path
            # and the tests that drive it do not pay for loading Textual.
            from .swtop_tui import run_app

            await run_app(collector, interval)


@click.command()
@click.argument("server_address", type=str)
@click.option(
    "--interval",
    "-i",
    type=float,
    default=DEFAULT_INTERVAL_S,
    show_default=True,
    help="Seconds between polls.",
)
@click.option(
    "--plain",
    is_flag=True,
    help="Print frames as text instead of running the terminal UI.",
)
def swtop(server_address: str, interval: float, plain: bool) -> None:
    """Watch the tasks and workers on the ds-service server at SERVER_ADDRESS.

    SERVER_ADDRESS is `host:port`, the same address an executor is given.
    Runs until interrupted.
    """
    if interval <= 0:
        raise click.BadParameter("must be greater than 0", param_hint="'--interval'")

    try:
        asyncio.run(watch(server_address, interval, plain))
    except KeyboardInterrupt:
        # A Ctrl-C that lands between two awaits comes out here,
        # rather than inside the loop it stops.
        pass
