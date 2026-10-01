"""Common utilities."""

from __future__ import annotations

import random
import string
import logging
from dataclasses import dataclass


def gen_random_string(k: int = 32) -> str:
    """A random string of `k` lowercase letters and digits."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))


def gen_error_id() -> str:
    """A fresh id to file one failure's traceback under."""
    return "ERROR_" + gen_random_string()


@dataclass
class RemoteExecutionError:
    """What a task's `output` holds when its worker raised.

    It is a value, not an exception: nothing raises it.
    `error` is the exception's message, as `str()` gives it.
    `error_id` appears verbatim beside the full traceback in that worker's log,
    under the executor's work dir.
    For a task that never ran because a task it waits on failed,
    `error` is the server's `Dependency failed (task_id=...)` text
    and `error_id` is empty.
    """

    error: str
    error_id: str


LOG_FORMAT: str = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
LOG_LEVEL: int = logging.INFO
