from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks_agent_client.memory_store import MemoryStoreClient


def _resource_id(name: str) -> str:
    return name.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class ManagedMemoryEntry:
    name: str
    actor_id: str
    path: str
    session_id: Optional[str] = None
    content: Optional[str] = None
    description: Optional[str] = None
    source_type: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @property
    def entry_id(self) -> str:
        return _resource_id(self.name)


@dataclass(frozen=True)
class ManagedMemoryStore:
    name: str
    display_name: str
    workspace_id: Optional[int] = None
    storage_backend: Optional[Dict[str, Any]] = None
    owner_user_id: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    description: Optional[str] = None
    _client: "MemoryStoreClient" = field(repr=False, compare=False, default=None)

    @property
    def store_id(self) -> str:
        return _resource_id(self.name)

    def add(
        self,
        *,
        actor_id: str,
        path: str,
        content: str,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> ManagedMemoryEntry:
        return self._client.add(
            self,
            actor_id=actor_id,
            path=path,
            content=content,
            session_id=session_id,
            description=description,
            source_type=source_type,
        )

    def append(
        self,
        *,
        actor_id: str,
        path: str,
        content: str,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> ManagedMemoryEntry:
        return self._client.append(
            self,
            actor_id=actor_id,
            path=path,
            content=content,
            session_id=session_id,
            description=description,
            source_type=source_type,
        )

    def list(
        self,
        *,
        actor_id: str,
        path_prefix: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[ManagedMemoryEntry]:
        return self._client.list(
            self,
            actor_id=actor_id,
            path_prefix=path_prefix,
            session_id=session_id,
        )

    def get(self, *, entry_id: str) -> ManagedMemoryEntry:
        return self._client.get_entry(self, entry_id=entry_id)

    def search(self, *, actor_id: str, query: str, limit: Optional[int] = None) -> List[ManagedMemoryEntry]:
        return self._client.search(self, actor_id=actor_id, query=query, limit=limit)

    def delete(self, *, entry_id: str) -> None:
        self._client.delete(self, entry_id=entry_id)
