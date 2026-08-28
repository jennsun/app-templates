from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING, Union

from databricks_agent_client._utils import _UNSET
from databricks_agent_client.models import ManagedMemoryStore

if TYPE_CHECKING:
    from databricks_agent_client.session_store import SessionStoreClient


@dataclass(frozen=True)
class SessionItem:
    item_id: str
    data: Any
    create_time: Optional[datetime] = None


@dataclass(frozen=True)
class Session:
    session_store_name: str
    session_id: str
    actor_id: str
    parent_session_id: Optional[str] = None
    root_session_id: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    last_activity_time: Optional[datetime] = None
    _client: "SessionStoreClient" = field(repr=False, compare=False, default=None)

    def update(self, *, metadata: Dict[str, str]) -> "Session":
        return self._client.update_session(self, metadata=metadata)

    def delete(self, *, force: bool = False) -> None:
        self._client.delete_session(self, force=force)

    def fork(
        self,
        *,
        actor_id: str,
        up_to_item_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> "Session":
        return self._client.fork_session(
            self,
            actor_id=actor_id,
            up_to_item_id=up_to_item_id,
            session_id=session_id,
            metadata=metadata,
        )

    def list_items(
        self,
        *,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[SessionItem]:
        return self._client.list_items(self, page_size=page_size, order_by=order_by)

    def append(self, items: Sequence[Any]) -> List[SessionItem]:
        return self._client.append_items(self, items=items)

    def pop(self) -> Optional[SessionItem]:
        return self._client.pop_item(self)

    def clear(self) -> None:
        self._client.clear_items(self)


@dataclass(frozen=True)
class SessionStore:
    session_store_name: str
    session_store_id: Optional[str] = None
    creator_user_id: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    description: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    _client: "SessionStoreClient" = field(repr=False, compare=False, default=None)

    def update(self, *, description: Any = _UNSET, metadata: Any = _UNSET) -> "SessionStore":
        return self._client.update(self, description=description, metadata=metadata)

    def delete(self) -> None:
        self._client.delete(self)

    def configure_distillation(
        self,
        *,
        memory_store: Union[str, ManagedMemoryStore],
        actor_id: str,
        schedule_period_minutes: int,
    ) -> int:
        return self._client.configure_distillation(
            self,
            memory_store=memory_store,
            actor_id=actor_id,
            schedule_period_minutes=schedule_period_minutes,
        )

    def create_session(
        self,
        *,
        actor_id: str,
        session_id: Optional[str] = None,
        parent_session_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Session:
        return self._client.create_session(
            self,
            actor_id=actor_id,
            session_id=session_id,
            parent_session_id=parent_session_id,
            metadata=metadata,
        )

    def list_sessions(
        self,
        *,
        page_size: Optional[int] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[Session]:
        return self._client.list_sessions(
            self,
            page_size=page_size,
            filter=filter,
            order_by=order_by,
        )

    def get_session(self, *, session_id: str) -> Session:
        return self._client.get_session(self, session_id=session_id)
