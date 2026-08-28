from typing import Any, Dict, List, Optional

from databricks_agent_client._utils import _parse_timestamp, _without_none
from databricks_agent_client.models import ManagedMemoryEntry, ManagedMemoryStore


_API_ROOT = "/api/agents/v1"


class MemoryStoreClient:
    def __init__(self, api_client: Any):
        self._api_client = api_client

    def create(self, *, display_name: str, description: Optional[str] = None) -> ManagedMemoryStore:
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/memory-stores",
            body=_without_none({"display_name": display_name, "description": description}),
        )
        return self._from_response(response)

    def get(
        self,
        *,
        store_id: Optional[str] = None,
        display_name: Optional[str] = None,
        create_if_not_exists: bool = False,
        description: Optional[str] = None,
    ) -> ManagedMemoryStore:
        if (store_id is None) == (display_name is None):
            raise ValueError("exactly one of store_id and display_name is required")
        if create_if_not_exists and store_id is not None:
            raise ValueError("create_if_not_exists requires display_name")
        if store_id is not None:
            response = self._api_client.do(method="GET", path=f"{_API_ROOT}/memory-stores/{store_id}")
            return self._from_response(response)

        for store in self._list_stores():
            if store.display_name == display_name:
                return store
        if create_if_not_exists:
            return self.create(display_name=display_name, description=description)
        raise KeyError(f"managed memory store not found: {display_name}")

    def _list_stores(self) -> List[ManagedMemoryStore]:
        stores = []
        page_token = None
        while True:
            query = _without_none({"page_token": page_token})
            response = self._api_client.do(method="GET", path=f"{_API_ROOT}/memory-stores", query=query)
            stores.extend(self._from_response(store) for store in response.get("managed_memory_stores", []))
            page_token = response.get("next_page_token")
            if not page_token:
                return stores

    def _from_response(self, response: Dict[str, Any]) -> ManagedMemoryStore:
        return ManagedMemoryStore(
            name=response["name"],
            display_name=response["display_name"],
            workspace_id=response.get("workspace_id"),
            storage_backend=response.get("storage_backend"),
            owner_user_id=response.get("owner_user_id"),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            description=response.get("description"),
            _client=self,
        )

    def add(
        self,
        store: ManagedMemoryStore,
        *,
        actor_id: str,
        path: str,
        content: str,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> ManagedMemoryEntry:
        entry = _without_none(
            {
                "actor_id": actor_id,
                "session_id": session_id,
                "path": path,
                "content": content,
                "description": description,
                "source_type": source_type,
            }
        )
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/{store.name}/entries",
            body=entry,
        )
        return self._entry_from_response(response)

    def list(
        self,
        store: ManagedMemoryStore,
        *,
        actor_id: str,
        path_prefix: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[ManagedMemoryEntry]:
        entries = []
        page_token = None
        while True:
            query = _without_none(
                {
                    "actor_id": actor_id,
                    "session_id": session_id,
                    "path_prefix": path_prefix,
                    "page_token": page_token,
                }
            )
            response = self._api_client.do(method="GET", path=f"{_API_ROOT}/{store.name}/entries", query=query)
            entries.extend(self._entry_from_response(entry) for entry in response.get("managed_memory_entries", []))
            page_token = response.get("next_page_token")
            if not page_token:
                return entries

    def get_entry(self, store: ManagedMemoryStore, *, entry_id: str) -> ManagedMemoryEntry:
        response = self._api_client.do(method="GET", path=f"{_API_ROOT}/{store.name}/entries/{entry_id}")
        return self._entry_from_response(response)

    def search(
        self,
        store: ManagedMemoryStore,
        *,
        actor_id: str,
        query: str,
        limit: Optional[int] = None,
    ) -> List[ManagedMemoryEntry]:
        response = self._api_client.do(
            method="POST",
            path=f"{_API_ROOT}/{store.name}/entries:search",
            body=_without_none({"actor_id": actor_id, "query": query, "limit": limit}),
        )
        return [self._entry_from_response(entry) for entry in response.get("managed_memory_entries", [])]

    def delete(self, store: ManagedMemoryStore, *, entry_id: str) -> None:
        self._api_client.do(method="DELETE", path=f"{_API_ROOT}/{store.name}/entries/{entry_id}")

    def append(
        self,
        store: ManagedMemoryStore,
        *,
        actor_id: str,
        path: str,
        content: str,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> ManagedMemoryEntry:
        matches = self.list(
            store,
            actor_id=actor_id,
            session_id=session_id,
            path_prefix=path,
        )
        exact_match = next(
            (
                entry
                for entry in matches
                if entry.actor_id == actor_id and entry.session_id == session_id and entry.path == path
            ),
            None,
        )
        if exact_match is None:
            return self.add(
                store,
                actor_id=actor_id,
                session_id=session_id,
                path=path,
                content=content,
                description=description,
                source_type=source_type,
            )

        current = self.get_entry(store, entry_id=exact_match.entry_id)
        updated_description = current.description if description is None else description
        updated_entry = _without_none(
            {
                "name": current.name,
                "actor_id": current.actor_id,
                "session_id": current.session_id,
                "path": current.path,
                "content": (current.content or "") + content,
                "description": updated_description,
            }
        )
        response = self._api_client.do(
            method="PATCH",
            path=f"{_API_ROOT}/{current.name}",
            body=updated_entry,
        )
        return self._entry_from_response(response)

    @staticmethod
    def _entry_from_response(response: Dict[str, Any]) -> ManagedMemoryEntry:
        return ManagedMemoryEntry(
            name=response["name"],
            actor_id=response["actor_id"],
            session_id=response.get("session_id"),
            path=response["path"],
            content=response.get("content"),
            description=response.get("description"),
            source_type=response.get("source_type"),
            create_time=_parse_timestamp(response.get("create_time")),
            update_time=_parse_timestamp(response.get("update_time")),
        )
