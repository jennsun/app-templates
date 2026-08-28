import re
from dataclasses import dataclass
from typing import Any, Union
from urllib.parse import quote

from databricks_agent_client.models import ManagedMemoryStore


_MEMORY_STORE_API_ROOT = "/api/agents/v1/memory-stores"
_LAKEBASE_BACKEND_TYPE = "STORAGE_BACKEND_TYPE_LAKEBASE"
_LAKEBASE_DATABASE_PATTERN = re.compile(r"projects/([^/]+)/branches/([^/]+)/databases/([^/]+)")


@dataclass(frozen=True)
class LakebaseDatabase:
    project_id: str
    branch_id: str
    database_id: str


def resolve_memory_store_database(
    workspace_client: Any,
    memory_store: Union[str, ManagedMemoryStore],
) -> LakebaseDatabase:
    if isinstance(memory_store, str):
        store_id = memory_store.rsplit("/", 1)[-1]
        response = workspace_client.api_client.do(
            method="GET",
            path=f"{_MEMORY_STORE_API_ROOT}/{quote(store_id, safe='')}",
        )
        storage_backend = response.get("storage_backend")
    else:
        storage_backend = memory_store.storage_backend

    if not storage_backend:
        raise ValueError("managed memory store has no storage backend")
    backend_type = storage_backend.get("backend_type")
    if backend_type is not None and backend_type != _LAKEBASE_BACKEND_TYPE:
        raise ValueError("managed memory store is not Lakebase-backed")
    match = _LAKEBASE_DATABASE_PATTERN.fullmatch(storage_backend.get("backend_id", ""))
    if match is None:
        raise ValueError("managed memory store has a malformed Lakebase backend ID")
    return LakebaseDatabase(
        project_id=match.group(1),
        branch_id=match.group(2),
        database_id=match.group(3),
    )
