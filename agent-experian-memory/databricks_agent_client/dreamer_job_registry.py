import json
from pathlib import Path
from typing import Dict, Optional


_DEFAULT_REGISTRY_PATH = Path.home() / ".databricks-agent-client" / "dreamer_jobs.json"


class DreamerJobRegistry:
    def __init__(self, path: Optional[Path] = None):
        self._path = path or _DEFAULT_REGISTRY_PATH

    def get(
        self,
        *,
        workspace_host: str,
        session_store_name: str,
        memory_store_name: str,
        actor_id: str,
    ) -> Optional[int]:
        entries = self._read()
        return entries.get(self._key(workspace_host, session_store_name, memory_store_name, actor_id))

    def put(
        self,
        *,
        workspace_host: str,
        session_store_name: str,
        memory_store_name: str,
        actor_id: str,
        job_id: int,
    ) -> None:
        entries = self._read()
        entries[self._key(workspace_host, session_store_name, memory_store_name, actor_id)] = job_id
        self._write(entries)

    @staticmethod
    def _key(workspace_host: str, session_store_name: str, memory_store_name: str, actor_id: str) -> str:
        return "|".join([workspace_host, session_store_name, memory_store_name, actor_id])

    def _read(self) -> Dict[str, int]:
        if not self._path.exists():
            return {}
        return json.loads(self._path.read_text(encoding="utf-8"))

    def _write(self, entries: Dict[str, int]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(entries), encoding="utf-8")
