from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from databricks_agent_client.dreamer_job_registry import DreamerJobRegistry
from databricks_agent_client.dreamer_memory import resolve_memory_store_database
from databricks_agent_client.dreamer_workspace import NOTEBOOK_PATH, upload_dreamer_bundle
from databricks_agent_client.models import ManagedMemoryStore


class DreamerConfigurationError(RuntimeError):
    """Dreamer distillation-job setup failed after the job may already exist."""


@dataclass(frozen=True)
class DreamerSettings:
    memory_store: Union[str, ManagedMemoryStore]
    actor_id: str
    schedule_period_minutes: int


def validate_dreamer_settings(
    *,
    memory_store: Optional[Union[str, ManagedMemoryStore]],
    actor_id: Optional[str],
    schedule_period_minutes: Optional[int],
) -> DreamerSettings:
    if not isinstance(memory_store, (str, ManagedMemoryStore)) or not memory_store:
        raise ValueError("memory_store is required")
    if not isinstance(actor_id, str) or not actor_id:
        raise ValueError("actor_id is required")
    if (
        isinstance(schedule_period_minutes, bool)
        or not isinstance(schedule_period_minutes, int)
        or schedule_period_minutes < 1
    ):
        raise ValueError("schedule_period_minutes must be at least 1")
    return DreamerSettings(
        memory_store=memory_store,
        actor_id=actor_id,
        schedule_period_minutes=schedule_period_minutes,
    )


_SESSION_STORE_PROJECT = "databricks-internal-lakebase-agent-session-store"

_JOB_PARAMETER_DEFAULTS = {
    "model_endpoint": "databricks-claude-sonnet-5",
    "sessions_table": "sessions",
    "session_items_table": "session_items",
    "memory_entries_table": "memory.memory_entries",
    "bookmarks_table": "dreamer_bookmarks",
    "settle_seconds": "0",
    "distill_until": "",
    "dry_run": "false",
}


class DreamerJobConfigurator:
    def __init__(self, workspace_client: Any, *, registry: Optional[DreamerJobRegistry] = None):
        self._workspace_client = workspace_client
        self._api_client = workspace_client.api_client
        self._registry = registry or DreamerJobRegistry()

    def configure(self, *, session_store_name: str, settings: DreamerSettings) -> int:
        memory_store_database = resolve_memory_store_database(self._workspace_client, settings.memory_store)
        memory_store_name = f"{memory_store_database.project_id}/{memory_store_database.database_id}"
        # The dreamer-diy pipeline's lakebase.resolve_store() requires "<project>/<database>";
        # session_store_name alone is just the database name.
        session_store_parameter = f"{_SESSION_STORE_PROJECT}/{session_store_name}"
        upload_dreamer_bundle(self._workspace_client)
        workspace_host = self._workspace_client.config.host
        job_id = self._resolve_or_create_job(
            workspace_host=workspace_host,
            session_store_name=session_store_name,
            session_store_parameter=session_store_parameter,
            memory_store_name=memory_store_name,
            actor_id=settings.actor_id,
        )
        self._update_job(
            job_id=job_id,
            session_store_parameter=session_store_parameter,
            memory_store_name=memory_store_name,
            actor_id=settings.actor_id,
            schedule_period_minutes=settings.schedule_period_minutes,
        )
        return job_id

    def _resolve_or_create_job(
        self,
        *,
        workspace_host: str,
        session_store_name: str,
        session_store_parameter: str,
        memory_store_name: str,
        actor_id: str,
    ) -> int:
        cached_job_id = self._registry.get(
            workspace_host=workspace_host,
            session_store_name=session_store_name,
            memory_store_name=memory_store_name,
            actor_id=actor_id,
        )
        if cached_job_id is not None and self._job_exists(cached_job_id):
            return cached_job_id
        job_id = self._create_job(
            session_store_parameter=session_store_parameter,
            memory_store_name=memory_store_name,
            actor_id=actor_id,
        )
        self._registry.put(
            workspace_host=workspace_host,
            session_store_name=session_store_name,
            memory_store_name=memory_store_name,
            actor_id=actor_id,
            job_id=job_id,
        )
        return job_id

    def _job_exists(self, job_id: int) -> bool:
        try:
            self._api_client.do(method="GET", path="/api/2.1/jobs/get", query={"job_id": job_id})
            return True
        except (OSError, RuntimeError) as error:
            message = str(error).lower()
            if "not found" not in message and "does not exist" not in message:
                raise
            return False

    def _create_job(self, *, session_store_parameter: str, memory_store_name: str, actor_id: str) -> int:
        parameters: List[Dict[str, Any]] = [
            {"name": "session_store_name", "default": session_store_parameter},
            {"name": "memory_store_name", "default": memory_store_name},
            {"name": "actor_id", "default": actor_id},
        ]
        for name, default in _JOB_PARAMETER_DEFAULTS.items():
            parameters.append({"name": name, "default": default})
        response = self._api_client.do(
            method="POST",
            path="/api/2.1/jobs/create",
            body={
                "name": f"dreamer-distillation-{actor_id}",
                "parameters": parameters,
                "tasks": [
                    {
                        "task_key": "run_dreamer",
                        "notebook_task": {"notebook_path": NOTEBOOK_PATH},
                    }
                ],
            },
        )
        return response["job_id"]

    def _update_job(
        self,
        *,
        job_id: int,
        session_store_parameter: str,
        memory_store_name: str,
        actor_id: str,
        schedule_period_minutes: int,
    ) -> None:
        job = self._api_client.do(method="GET", path="/api/2.1/jobs/get", query={"job_id": job_id})
        settings = job.get("settings")
        if not isinstance(settings, dict):
            raise DreamerConfigurationError("job response has no settings")
        parameters = _merge_parameters(
            settings.get("parameters", []),
            session_store_name=session_store_parameter,
            memory_store_name=memory_store_name,
            actor_id=actor_id,
        )
        trigger = {
            "pause_status": "UNPAUSED",
            "periodic": {"interval": schedule_period_minutes, "unit": "MINUTES"},
        }
        self._api_client.do(
            method="POST",
            path="/api/2.1/jobs/update",
            body={"job_id": job_id, "new_settings": {"parameters": parameters, "trigger": trigger}},
        )


def _merge_parameters(
    parameters: Any,
    *,
    session_store_name: str,
    memory_store_name: str,
    actor_id: str,
) -> List[Dict[str, Any]]:
    replacements = {
        "session_store_name": session_store_name,
        "memory_store_name": memory_store_name,
        "actor_id": actor_id,
    }
    merged = []
    present_names = set()
    for parameter in parameters:
        parameter_copy = dict(parameter)
        name = parameter_copy["name"]
        present_names.add(name)
        if name in replacements:
            parameter_copy["default"] = replacements[name]
        merged.append(parameter_copy)
    for name, value in replacements.items():
        if name not in present_names:
            merged.append({"name": name, "default": value})
    return merged
