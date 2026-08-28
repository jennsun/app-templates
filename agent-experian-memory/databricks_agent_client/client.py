from typing import Any, Optional

from databricks_agent_client.memory_store import MemoryStoreClient
from databricks_agent_client.session_store import SessionStoreClient


def _default_workspace_client() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


class DatabricksAgentClient:
    """Client for Databricks agent APIs."""

    def __init__(self, workspace_client: Optional[Any] = None):
        workspace_client = workspace_client or _default_workspace_client()
        self.memory_store = MemoryStoreClient(workspace_client.api_client)
        self.session_store = SessionStoreClient(workspace_client.api_client, workspace_client=workspace_client)
