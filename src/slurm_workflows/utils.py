"""Common utilities."""

from __future__ import annotations

import math
import random
import string
import logging
from dataclasses import dataclass
from typing import Any, Mapping


def gen_random_string(k: int = 32) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))


def gen_error_id() -> str:
    return "ERROR_" + gen_random_string()


@dataclass
class RemoteExecutionError:
    error: str
    error_id: str


def objective_value(
    name: str, objective_key: str, params: Mapping[str, Any], output: Any
) -> float:
    """The value to rank one evaluation by, or a `RuntimeError` saying why not.

    Every rejection names what came back and at which point.
    """
    if not isinstance(output, Mapping):
        raise RuntimeError(
            f"{name}: objective returned {output!r} at {params}; "
            f"expected a mapping carrying an {objective_key!r} key"
        )
    if objective_key not in output:
        raise RuntimeError(
            f"{name}: objective returned keys {sorted(output)} "
            f"at {params}, with no {objective_key!r} among them"
        )

    try:
        value = float(output[objective_key])
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            f"{name}: {objective_key!r} was {output[objective_key]!r} "
            f"at {params}, which is not a float"
        ) from e

    if not math.isfinite(value):
        raise RuntimeError(
            f"{name}: {objective_key!r} was {value} at {params}; "
            "a non-finite value can be neither ranked nor modelled"
        )

    return value


def floor_power_of_two(n: int) -> int:
    """Largest power of two <= n."""
    if n < 1:
        raise ValueError(f"expected a positive integer, got {n}")
    return 1 << (n.bit_length() - 1)


def index_width(count: int) -> int:
    """Digits needed to number `count` things, so that the numbers sort."""
    return len(str(max(count - 1, 0)))


def format_param(value: Any) -> str:
    """Render one value for a progress line, floats at a fixed precision."""
    return f"{value:.6g}" if isinstance(value, float) else str(value)


def format_mapping(mapping: Mapping[str, Any]) -> str:
    """Render a whole mapping for a progress line."""
    return ", ".join(f"{k}={format_param(v)}" for k, v in mapping.items())


LOG_FORMAT: str = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
LOG_LEVEL = logging.INFO
