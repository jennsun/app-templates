import base64
import importlib.resources as resources
from typing import Any, Optional


_NOTEBOOK_DIR = "/Workspace/Shared/dreamer-diy/notebooks"
_SRC_DIR = "/Workspace/Shared/dreamer-diy/src/dreamer"
_NOTEBOOK_FILENAME = "dreamer_per_actor_memory_distillation.py"
NOTEBOOK_PATH = f"{_NOTEBOOK_DIR}/{_NOTEBOOK_FILENAME[:-3]}"


def upload_dreamer_bundle(workspace_client: Any) -> None:
    api_client = workspace_client.api_client
    _mkdirs(api_client, _NOTEBOOK_DIR)
    _mkdirs(api_client, _SRC_DIR)

    bundle_root = resources.files("databricks_agent_client").joinpath("_dreamer_bundle")
    notebook_content = bundle_root.joinpath("notebooks", _NOTEBOOK_FILENAME).read_bytes()
    _import_file(api_client, path=NOTEBOOK_PATH, content=notebook_content, format_="SOURCE", language="PYTHON")

    src_dir = bundle_root.joinpath("src", "dreamer")
    for entry in src_dir.iterdir():
        if not entry.name.endswith(".py"):
            continue
        _import_file(api_client, path=f"{_SRC_DIR}/{entry.name}", content=entry.read_bytes(), format_="AUTO")


def _mkdirs(api_client: Any, path: str) -> None:
    api_client.do(method="POST", path="/api/2.0/workspace/mkdirs", body={"path": path})


def _import_file(api_client: Any, *, path: str, content: bytes, format_: str, language: Optional[str] = None) -> None:
    body = {
        "path": path,
        "content": base64.b64encode(content).decode("ascii"),
        "format": format_,
        "overwrite": True,
    }
    if language is not None:
        body["language"] = language
    api_client.do(method="POST", path="/api/2.0/workspace/import", body=body)
