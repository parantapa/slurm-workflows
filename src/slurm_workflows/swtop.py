"""`swtop`: a live view of a `ds-service` task queue.

Polls the server and redraws a summary of its tasks, of the pilot jobs
the executor submitted, and of the worker processes running in them.
`swtop_tui.py` holds the terminal UI; this module decides what to show.

See `docs/how-to-use-swtop.md` for the blocks and what fills them,
and the monitoring section of `docs/developer-notes.md` for why they
are collected the way they are.
"""

from __future__ import annotations

import sys
import json
import asyncio
from typing import cast, AsyncIterator
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import click
from ds_service_client import DsServiceClientAsync, TaskState, TaskStateError

from .monitors import HOST_SERIES, JOB_SERIES
from .slurm_pilot_executor import (
    PROGRESS_DISPLAY_KEY,
    PROGRESS_SERIES_PREFIX,
    WORKER_JOB_INFO_PREFIX,
)
from .slurm_pilot_worker import WORKER_PROCESS_INFO_PREFIX

DEFAULT_INTERVAL_S: float = 2.0

# The fields a worker process publishes about itself,
# in the order it writes them; the tables order their own columns.
WORKER_INFO_FIELDS = ["group", "name", "slurm_job_id", "hostname", "pid"]

# The fields the executor publishes about a pilot job, likewise.
WORKER_JOB_FIELDS = ["name", "group", "slurm_job_id", "submit_time"]

# The fields of the progress display a wait publishes.
PROGRESS_FIELDS = ["progress_id", "desc", "unit", "total"]

# How far back a progress reading is still shown as live.
PROGRESS_TAIL_S = 60.0

TASK_NAME_PREFIX = "task_name:"

# `task_search_id` matches task ids against a regular expression,
# and an empty one matches every task the server holds.
ALL_TASK_IDS = ""

# Shown in place of the name of a task that was never given one.
UNNAMED = "-"

# How far back a monitored value is still worth showing.
# A monitor samples every 5 seconds, so nothing this recent is stale.
STALE_AFTER_S = 60.0

# Shown when a worker published its id but not the field being read,
# which is what a worker caught mid-startup looks like.
UNKNOWN = "?"

# The order tasks are listed in: what is happening now, first.
STATE_ORDER = ["Running", "Ready", "Complete", "Canceled", "Undefined"]

# What each block says when it has nothing to show.
# Each says why it is empty, since an empty block is usually a question.
EMPTY_WORKER_JOBS = "no pilot jobs have been submitted through this server"
EMPTY_WORKERS = "no worker processes have registered with this server"
EMPTY_HOSTS = "no host is being monitored"
EMPTY_JOBS = "no slurm job is being monitored"
EMPTY_TASKS = "no tasks have been submitted to this server"


@dataclass
class WorkerInfo:
    """One registered worker process, as it describes itself in the store."""

    worker_id: str
    group: str
    name: str
    slurm_job_id: str
    hostname: str
    pid: str


@dataclass
class WorkerJobInfo:
    """One pilot job, as the executor described it when it submitted it."""

    name: str
    group: str
    slurm_job_id: str
    submit_time: str


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
    """The latest reading of one monitored host or job.

    `values` is empty when the subject's series exist
    but hold nothing recent, which is what a dead monitor looks like.
    """

    subject: str
    values: dict[str, float] = field(default_factory=dict)

    @property
    def stale(self) -> bool:
        return not self.values


@dataclass
class ProgressInfo:
    """What a `wait` or `as_completed` call is working through."""

    progress_id: str
    desc: str
    unit: str
    total: int
    completed: int = 0

    @property
    def done(self) -> bool:
        return self.completed >= self.total

    @property
    def fraction(self) -> float:
        return self.completed / self.total if self.total else 1.0


@dataclass
class Snapshot:
    """One poll's worth of server state."""

    address: str
    when: datetime
    counts: dict[str, int] = field(default_factory=dict)
    progress: ProgressInfo | None = None
    worker_jobs: list[WorkerJobInfo] = field(default_factory=list)
    workers: list[WorkerInfo] = field(default_factory=list)
    tasks: list[TaskInfo] = field(default_factory=list)
    hosts: list[SubjectInfo] = field(default_factory=list)
    jobs: list[SubjectInfo] = field(default_factory=list)
    error: str | None = None


class Collector:
    """Turns the server's RPCs into a `Snapshot`.

    Caches the identities it has read, which are written once and never
    change, so a steady state re-reads only what is new.
    """

    def __init__(self, client: DsServiceClientAsync, address: str) -> None:
        self.client = client
        self.address = address
        self._worker_jobs: dict[str, WorkerJobInfo] = {}
        self._workers: dict[str, WorkerInfo] = {}
        # The last count read for a progress id, so a display that has
        # stopped moving is still drawn where it stopped.
        self._progress_seen: dict[str, int] = {}
        self._task_names: dict[str, str] = {}

    async def snapshot(self) -> Snapshot:
        # None of these six needs an answer from another,
        # so they go out together and the poll waits once.
        counts, progress, worker_jobs, workers, hosts, jobs = await asyncio.gather(
            self.client.task_get_count_by_state(),
            self._collect_progress(),
            self._collect_worker_jobs(),
            self._collect_workers(),
            self._collect_subjects(HOST_SERIES),
            self._collect_subjects(JOB_SERIES),
        )
        # The tasks do need the workers:
        # a running task is labelled with the name of the worker holding it.
        tasks = await self._collect_tasks(workers)

        return Snapshot(
            address=self.address,
            when=datetime.now(),
            counts={
                "ready": counts.ready,
                "running": counts.running,
                "complete": counts.complete,
                "canceled": counts.canceled,
            },
            progress=progress,
            worker_jobs=worker_jobs,
            workers=workers,
            tasks=tasks,
            hosts=hosts,
            jobs=jobs,
        )

    async def _text(self, key: str) -> str:
        try:
            value = await self.client.map_get(key)
        except KeyError:
            return UNKNOWN
        return value.decode("utf-8", errors="replace")

    async def _collect_progress(self) -> ProgressInfo | None:
        """The progress display a wait published, and how far it has got."""
        text = await self._text(PROGRESS_DISPLAY_KEY)
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

    async def _collect_worker_jobs(self) -> list[WorkerJobInfo]:
        """Every pilot job the executor has published, cached like the rest."""
        names = [
            key[len(WORKER_JOB_INFO_PREFIX) :]
            for key in await self.client.map_search_key(f"^{WORKER_JOB_INFO_PREFIX}")
        ]

        missing = [n for n in names if n not in self._worker_jobs]
        read = await asyncio.gather(*(self._worker_job_info(n) for n in missing))
        for name, info in zip(missing, read):
            if info is not None:
                self._worker_jobs[name] = info

        listed = [
            self._worker_jobs.get(name) or _unknown_worker_job(name) for name in names
        ]
        return sorted(listed, key=lambda j: (j.group, j.name))

    async def _worker_job_info(self, name: str) -> WorkerJobInfo | None:
        """One job's published description, or None if it is not readable."""
        text = await self._text(f"{WORKER_JOB_INFO_PREFIX}{name}")
        try:
            published = json.loads(text)
            fields = {field: str(published[field]) for field in WORKER_JOB_FIELDS}
        except (ValueError, TypeError, KeyError):
            return None

        return WorkerJobInfo(**fields)

    async def _collect_workers(self) -> list[WorkerInfo]:
        worker_ids = [
            key[len(WORKER_PROCESS_INFO_PREFIX) :]
            for key in await self.client.map_search_key(
                f"^{WORKER_PROCESS_INFO_PREFIX}"
            )
        ]

        # Every description the cache is short of, read in one go.
        missing = [w for w in worker_ids if w not in self._workers]
        read = await asyncio.gather(*(self._worker_info(w) for w in missing))
        for worker_id, info in zip(missing, read):
            if info is not None:
                # Not cached: it may be a write this read landed in the middle of.
                self._workers[worker_id] = info

        listed = [
            self._workers.get(worker_id) or _unknown_worker(worker_id)
            for worker_id in worker_ids
        ]
        return sorted(listed, key=lambda w: (w.group, w.name))

    async def _worker_info(self, worker_id: str) -> WorkerInfo | None:
        """One worker's published description, or None if it is not readable."""
        text = await self._text(f"{WORKER_PROCESS_INFO_PREFIX}{worker_id}")
        try:
            published = json.loads(text)
            fields = {name: str(published[name]) for name in WORKER_INFO_FIELDS}
        except (ValueError, TypeError, KeyError):
            return None

        return WorkerInfo(worker_id=worker_id, **fields)

    async def _collect_tasks(self, workers: list[WorkerInfo]) -> list[TaskInfo]:
        # Which tasks there are, and which of them have been named:
        # two searches, neither of which needs the other's answer.
        task_ids, name_keys = await asyncio.gather(
            self.client.task_search_id(ALL_TASK_IDS),
            self.client.map_search_key(f"^{TASK_NAME_PREFIX}"),
        )
        if not task_ids:
            return []

        # Only names that were there are cached: a task seen before
        # `set_task_name` ran may have been named since.
        named = {key[len(TASK_NAME_PREFIX) :] for key in name_keys}
        missing = sorted(named - self._task_names.keys())
        names = await asyncio.gather(
            *(self._text(f"{TASK_NAME_PREFIX}{task_id}") for task_id in missing)
        )
        self._task_names.update(zip(missing, names))

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
                state=TaskState.Name(state),
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
        # a name is what somebody wanted to be able to find.
        return sorted(
            tasks,
            key=lambda t: (_state_rank(t.state), t.name == UNNAMED, t.name, t.task_id),
        )

    async def _collect_subjects(self, prefixes: dict[str, str]) -> list[SubjectInfo]:
        """The latest reading of every subject one monitor writes about.

        The subjects are discovered from one of the series.
        """
        first = next(iter(prefixes.values()))
        subjects = sorted(
            key[len(first) :]
            for key in await self.client.time_series_search_key(f"^{first}")
        )
        if not subjects:
            return []

        # The tail only: a whole series grows without bound over a run.
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
        """The name of the worker running `task_id`, or "" if it cannot be told."""
        try:
            worker_id = await self.client.task_get_worker_id(task_id)
        except (KeyError, TaskStateError):
            return ""
        return worker_names.get(worker_id, worker_id)


@asynccontextmanager
async def open_collector(address: str) -> AsyncIterator[Collector]:
    """A collector on a client of its own, closed on the way out.

    Must be entered on the event loop the client is to belong to.
    """
    async with DsServiceClientAsync(address) as client:
        yield Collector(client, address)


def _unknown_worker(worker_id: str) -> WorkerInfo:
    """A row for a worker process whose description could not be read."""
    return WorkerInfo(
        worker_id=worker_id, **{name: UNKNOWN for name in WORKER_INFO_FIELDS}
    )


def _unknown_worker_job(name: str) -> WorkerJobInfo:
    """A row for a pilot job whose description could not be read."""
    return WorkerJobInfo(
        **{field: UNKNOWN for field in WORKER_JOB_FIELDS} | {"name": name}
    )


def _state_rank(state: str) -> int:
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
    return f"{value:.1f}T"


def _subject(info: SubjectInfo) -> str:
    """The subject's name, said to be stale when its series have stopped."""
    return info.subject if not info.stale else f"{info.subject} (stale)"


def _cell(values: dict[str, float], name: str, fmt) -> str:
    """One measurement, or a dash where the series had nothing recent."""
    if name not in values:
        return "-"
    return fmt(values[name])


# The columns of each block, shared by the text frames and the UI.
WORKER_JOB_COLUMNS = ["NAME", "GROUP", "JOB", "SUBMITTED"]
WORKER_COLUMNS = ["NAME", "GROUP", "HOST", "JOB", "PID"]
HOST_COLUMNS = ["HOST", "FREE MEM", "LOAD", "/dev/shm", "/tmp"]
JOB_COLUMNS = ["JOB", "MEMORY", "CPU"]
TASK_COLUMNS = ["NAME", "TASK ID", "STATE", "WORKER"]


def progress_line(snapshot: Snapshot, width: int = 24) -> str:
    """The progress display as one line, or "" when nothing published one."""
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


def worker_job_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per submitted pilot job, keyed by its worker name."""
    return [
        (job.name, [job.name, job.group, job.slurm_job_id, job.submit_time])
        for job in snapshot.worker_jobs
    ]


def worker_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per registered worker process, keyed by its worker id."""
    return [
        (w.worker_id, [w.name, w.group, w.hostname, w.slurm_job_id, w.pid])
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
    """One row per monitored Slurm job, keyed by its job id."""
    return [
        (
            job.subject,
            [
                _subject(job),
                _cell(job.values, "memory", _bytes),
                _cell(job.values, "cpu", lambda v: f"{v:.1f} cores"),
            ],
        )
        for job in snapshot.jobs
    ]


def task_rows(snapshot: Snapshot) -> list[tuple[str, list[str]]]:
    """One row per task, keyed by its task id."""
    return [
        (task.task_id, [task.name, task.task_id, task.state, task.worker])
        for task in snapshot.tasks
    ]


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

    blocks = [
        (
            "worker jobs",
            WORKER_JOB_COLUMNS,
            worker_job_rows(snapshot),
            EMPTY_WORKER_JOBS,
        ),
        ("worker processes", WORKER_COLUMNS, worker_rows(snapshot), EMPTY_WORKERS),
        ("hosts", HOST_COLUMNS, host_rows(snapshot), EMPTY_HOSTS),
        ("slurm jobs", JOB_COLUMNS, job_rows(snapshot), EMPTY_JOBS),
        ("tasks", TASK_COLUMNS, task_rows(snapshot), EMPTY_TASKS),
    ]
    for title, columns, rows, empty in blocks:
        lines.append(f"{title} ({len(rows)})")
        if rows:
            lines.extend(_table(columns, [cells for _, cells in rows]))
        else:
            lines.append(empty)
        lines.append("")

    return "\n".join(lines[:-1]) + "\n"


def draw(text: str) -> None:
    """Put `text` on the screen, replacing what was there.

    Redirected output is appended instead, without escape codes.
    """
    if sys.stdout.isatty():
        # Home, then clear: clearing first leaves the old frame visible
        # for a moment on a slow link.
        sys.stdout.write("\x1b[H\x1b[2J")
    sys.stdout.write(text)
    if not sys.stdout.isatty():
        sys.stdout.write("\n")
    sys.stdout.flush()


async def run_plain(collector: Collector, interval: float) -> None:
    """Poll and print frames until interrupted."""
    try:
        while True:
            try:
                snapshot = await collector.snapshot()
            except Exception as e:
                # A server that is down, or not up yet, is waited out.
                snapshot = Snapshot(
                    address=collector.address,
                    when=datetime.now(),
                    error=f"{type(e).__name__}: {e}",
                )

            draw(render(snapshot))
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        # Ctrl-C is how this is meant to end.
        pass


async def watch(server_address: str, interval: float, plain: bool) -> None:
    """Open a client on this loop and run whichever display was asked for."""
    async with open_collector(server_address) as collector:
        if plain or not sys.stdout.isatty():
            await run_plain(collector, interval)
        else:
            # Imported here so the text path, and the tests that drive it,
            # do not pay for loading Textual.
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
        # A Ctrl-C that lands between two awaits comes out here
        # rather than inside the loop that was asked to stop.
        pass
