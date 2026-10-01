"""Helpers shared by the `map_reduce` and `map` tests."""

from __future__ import annotations

from typing import Any

from ds_service_client import DsServiceClient

from slurm_workflows.swtop import ALL_TASK_IDS


class RecordingClient:
    """Records the order of the calls a test asserts on."""

    def __init__(self, inner: DsServiceClient) -> None:
        self._inner = inner
        self.added_ids: list[str] = []
        self.parent_ids: dict[str, list[str]] = {}
        self.output_reads: list[str] = []

    def task_add(self, task_id: str, **kwargs: Any) -> None:
        self.added_ids.append(task_id)
        self.parent_ids[task_id] = list(kwargs.get("parent_task_ids", []))
        return self._inner.task_add(task_id=task_id, **kwargs)

    def task_get_output(self, task_id: str) -> bytes:
        self.output_reads.append(task_id)
        return self._inner.task_get_output(task_id)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def item_ids(ds_client: DsServiceClient) -> list[str]:
    """Every item task on the server, in id order."""
    return sorted(i for i in ds_client.task_search_id(ALL_TASK_IDS) if ".item." in i)
