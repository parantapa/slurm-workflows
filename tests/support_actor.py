"""Actor classes for the actor tests.

Workers resolve actors with `importlib`,
so these must be importable by name (`support_actor.CounterActor`).
conftest puts this directory on `sys.path`.
"""

from __future__ import annotations

# Instances created in this process, so tests can assert on per-worker state.
INSTANCES: list["CounterActor"] = []


def reset() -> None:
    INSTANCES.clear()


class CounterActor:
    """Keeps state across tasks, and records that `close()` ran."""

    def __init__(self) -> None:
        self.calls = 0
        self.closed = False
        INSTANCES.append(self)

    def bump(self, n: int = 1) -> int:
        self.calls += n
        return self.calls

    def echo(self, value):
        return value

    def boom(self):
        raise ValueError("actor failure")

    def close(self) -> None:
        self.closed = True


class ConfiguredActor:
    """Keeps the constructor arguments the caller passes."""

    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def config(self) -> tuple[tuple, dict]:
        return self.args, self.kwargs


class NoCloseActor:
    """Has no close(). Exercises the optional-cleanup branch."""

    def ping(self) -> str:
        return "pong"


class MapActor:
    """Maps one item with state built once, for the mapreduce tests."""

    def __init__(self, factor: int = 1) -> None:
        self.factor = factor
        self.calls = 0

    def scale(self, x: int) -> int:
        self.calls += 1
        return x * self.factor

    def offset(self, x: int, delta: int, sign: int = 1) -> int:
        return (x * self.factor + delta) * sign

    def explode(self, x: int) -> int:
        raise ValueError(f"no good: {x}")


class EnvironmentActor:
    """Reads the worker environment at construction, as a real actor can."""

    def __init__(self) -> None:
        import os

        self.server_address = os.environ["DS_SERVER_ADDRESS"]
        self.worker_id = os.environ["PILOT_WORKER_ID"]
