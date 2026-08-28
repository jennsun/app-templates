from typing import Any, Dict, List, Optional, Sequence, Union

from databricks_agent_client._utils import _parse_timestamp, _UNSET, _without_none
from databricks_agent_client.dreamer_job import (
    DreamerConfigurationError,
    DreamerJobConfigurator,
    validate_dreamer_settings,
)
from databricks_agent_client.models import ManagedMemoryStore
from databricks_agent_client.session_models import Session, SessionItem, SessionStore


_API_ROOT = "/api/agents/v1"


class SessionStoreClient:
    def __init__(
        self,
        api_client: Any,
        *,
        workspace_client: Optional[Any] = None,
        dreamer_configurator: Optional[Any] = None,
    ):
        self._api_client = api_client
        if dreamer_configurator is not None:
            self._dreamer_configurator = dreamer_configurator
        elif workspace_client is not None:
            self._dreamer_configurator = DreamerJobConfigurator(workspace_client)
        else:
            self._dreamer_configurator = None

    def set_session_store(self, session_store_name: str) -> SessionStore:
        if not session_store_name:
            raise ValueError("session_store_name is required")
        return SessionStore(session_store_name=session_store_name, _client=self)

    def create(
        self,
        *,
        session_store_name: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        memory_store: Optional[Union[str, ManagedMemoryStore]] = None,
        enable_distillation: bool = False,
        actor_id: Optional[str] = None,
        schedule_period_minutes: Optional[int] = None,
    ) -> SessionStore:
        dreamer_settings = None
        dreamer_configurator = None
        if enable_distillation:
            dreamer_settings = validate_dreamer_settings(
                memory_store=memory_store,
                actor_id=actor_id,
                schedule_period_minutes=schedule_period_minutes,
            )
            dreamer_configurator = self._require_dreamer_configurator()
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/session-stores",
            query={"session_store_name": session_store_name},
            body=_without_none({"description": description, "metadata": metadata}),
        )
        store = self._store_from_response(response)
        if dreamer_settings is not None and dreamer_configurator is not None:
            try:
                dreamer_configurator.configure(
                    session_store_name=store.session_store_name,
                    settings=dreamer_settings,
                )
            except Exception as error:
                raise DreamerConfigurationError(
                    f"session store {store.session_store_name!r} was created, "
                    "but distillation job setup failed; retry with "
                    f"store.configure_distillation(...): {error}"
                ) from error
        return store

    def configure_distillation(
        self,
        store: SessionStore,
        *,
        memory_store: Union[str, ManagedMemoryStore],
        actor_id: str,
        schedule_period_minutes: int,
    ) -> int:
        settings = validate_dreamer_settings(
            memory_store=memory_store,
            actor_id=actor_id,
            schedule_period_minutes=schedule_period_minutes,
        )
        configurator = self._require_dreamer_configurator()
        return configurator.configure(
            session_store_name=store.session_store_name,
            settings=settings,
        )

    def _require_dreamer_configurator(self) -> Any:
        if self._dreamer_configurator is None:
            raise RuntimeError(
                "distillation setup requires SessionStoreClient to be constructed with a workspace_client"
            )
        return self._dreamer_configurator

    def list(self, *, page_size: Optional[int] = None) -> List[SessionStore]:
        stores = []
        page_token = None
        while True:
            query = _without_none({"page_size": page_size, "page_token": page_token})
            response = self._api_client.do(
                method="GET",
                path=f"{_API_ROOT}/session-stores",
                query=query,
            )
            stores.extend(self._store_from_response(store) for store in response.get("session_stores", []))
            page_token = response.get("next_page_token")
            if not page_token:
                return stores

    def get(self, *, session_store_name: str) -> SessionStore:
        response = self._api_client.do(
            method="GET",
            path=f"{_API_ROOT}/session-stores/{session_store_name}",
        )
        return self._store_from_response(response)

    def update(
        self,
        store: SessionStore,
        *,
        description: Any = _UNSET,
        metadata: Any = _UNSET,
    ) -> SessionStore:
        body = {}
        update_mask = []
        if description is not _UNSET:
            body["description"] = description
            update_mask.append("description")
        if metadata is not _UNSET:
            body["metadata"] = metadata
            update_mask.append("metadata")
        if not update_mask:
            raise ValueError("at least one of description and metadata is required")

        response = self._api_client.do(
            method="PATCH",
            path=f"{_API_ROOT}/session-stores/{store.session_store_name}",
            query={"update_mask": ",".join(update_mask)},
            body=body,
        )
        return self._store_from_response(response)

    def delete(self, store: SessionStore) -> None:
        self._api_client.do(
            method="DELETE",
            path=f"{_API_ROOT}/session-stores/{store.session_store_name}",
        )

    def create_session(
        self,
        store: Optional[SessionStore] = None,
        *,
        session_store_name: Optional[str] = None,
        actor_id: str,
        session_id: Optional[str] = None,
        parent_session_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Session:
        resolved_store_name = self._resolve_session_store_name(store, session_store_name)
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/session-stores/{resolved_store_name}/sessions",
            query=_without_none({"session_id": session_id}),
            body=_without_none(
                {
                    "actor_id": actor_id,
                    "parent_session_id": parent_session_id,
                    "metadata": metadata,
                }
            ),
        )
        return self._session_from_response(response, resolved_store_name)

    def list_sessions(
        self,
        store: Optional[SessionStore] = None,
        *,
        session_store_name: Optional[str] = None,
        page_size: Optional[int] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[Session]:
        resolved_store_name = self._resolve_session_store_name(store, session_store_name)
        sessions = []
        page_token = None
        while True:
            query = _without_none(
                {
                    "page_size": page_size,
                    "page_token": page_token,
                    "filter": filter,
                    "order_by": order_by,
                }
            )
            response = self._api_client.do(
                method="GET",
                path=f"{_API_ROOT}/session-stores/{resolved_store_name}/sessions",
                query=query,
            )
            sessions.extend(
                self._session_from_response(session, resolved_store_name) for session in response.get("sessions", [])
            )
            page_token = response.get("next_page_token")
            if not page_token:
                return sessions

    def get_session(
        self,
        store: Optional[SessionStore] = None,
        *,
        session_store_name: Optional[str] = None,
        session_id: str,
    ) -> Session:
        resolved_store_name = self._resolve_session_store_name(store, session_store_name)
        response = self._api_client.do(
            method="GET",
            path=f"{_API_ROOT}/session-stores/{resolved_store_name}/sessions/{session_id}",
        )
        return self._session_from_response(response, resolved_store_name)

    def update_session(self, session: Session, *, metadata: Dict[str, str]) -> Session:
        response = self._api_client.do(
            method="PATCH",
            path=self._session_path(session),
            query={"update_mask": "metadata"},
            body={"metadata": metadata},
        )
        return self._session_from_response(response, session.session_store_name)

    def delete_session(self, session: Session, *, force: bool = False) -> None:
        request: Dict[str, Any] = {"method": "DELETE", "path": self._session_path(session)}
        if force:
            request["query"] = {"force": True}
        self._api_client.do(**request)

    def fork_session(
        self,
        session: Session,
        *,
        actor_id: str,
        up_to_item_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Session:
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/session-stores/{session.session_store_name}/sessions:fork",
            body=_without_none(
                {
                    "source_session_id": session.session_id,
                    "up_to_item_id": up_to_item_id,
                    "session_id": session_id,
                    "actor_id": actor_id,
                    "metadata": metadata,
                }
            ),
        )
        return self._session_from_response(response["session"], session.session_store_name)

    def list_items(
        self,
        session: Session,
        *,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[SessionItem]:
        items = []
        page_token = None
        while True:
            query = _without_none(
                {
                    "page_size": page_size,
                    "page_token": page_token,
                    "order_by": order_by,
                }
            )
            response = self._api_client.do(
                method="GET",
                path=f"{self._session_path(session)}/items",
                query=query,
            )
            items.extend(self._item_from_response(item) for item in response.get("session_items", []))
            page_token = response.get("next_page_token")
            if not page_token:
                return items

    def append_items(self, session: Session, *, items: Sequence[Any]) -> List[SessionItem]:
        if not items:
            raise ValueError("at least one item is required")
        response = self._api_client.do(
            method="POST",
            path=f"{self._session_path(session)}/items:append",
            body={"items": [{"data": item} for item in items]},
        )
        return [self._item_from_response(item) for item in response.get("session_items", [])]

    def pop_item(self, session: Session) -> Optional[SessionItem]:
        response = self._api_client.do(
            method="POST",
            path=f"{self._session_path(session)}/items:pop",
            body={},
        )
        item = response.get("item")
        return self._item_from_response(item) if item is not None else None

    def clear_items(self, session: Session) -> None:
        self._api_client.do(
            method="POST",
            path=f"{self._session_path(session)}/items:clear",
            body={},
        )

    @staticmethod
    def _resolve_session_store_name(
        store: Optional[SessionStore],
        session_store_name: Optional[str],
    ) -> str:
        if store is None:
            if not session_store_name:
                raise ValueError("session_store_name is required")
            return session_store_name
        if session_store_name is not None and session_store_name != store.session_store_name:
            raise ValueError("session_store_name conflicts with the bound session store")
        return store.session_store_name

    @staticmethod
    def _session_path(session: Session) -> str:
        return f"{_API_ROOT}/session-stores/{session.session_store_name}/sessions/{session.session_id}"

    def _store_from_response(self, response: Dict[str, Any]) -> SessionStore:
        return SessionStore(
            session_store_name=response["session_store_name"],
            session_store_id=response.get("session_store_id"),
            creator_user_id=response.get("creator_user_id"),
            create_time=_parse_timestamp(response.get("create_time")),
            update_time=_parse_timestamp(response.get("update_time")),
            description=response.get("description"),
            metadata=dict(response.get("metadata", {})),
            _client=self,
        )

    def _session_from_response(
        self,
        response: Dict[str, Any],
        session_store_name: str,
    ) -> Session:
        return Session(
            session_store_name=response.get("session_store_name", session_store_name),
            session_id=response["session_id"],
            actor_id=response["actor_id"],
            parent_session_id=response.get("parent_session_id"),
            root_session_id=response.get("root_session_id"),
            metadata=dict(response.get("metadata", {})),
            create_time=_parse_timestamp(response.get("create_time")),
            update_time=_parse_timestamp(response.get("update_time")),
            last_activity_time=_parse_timestamp(response.get("last_activity_time")),
            _client=self,
        )

    @staticmethod
    def _item_from_response(response: Dict[str, Any]) -> SessionItem:
        return SessionItem(
            item_id=response["item_id"],
            data=response["data"],
            create_time=_parse_timestamp(response.get("create_time")),
        )
