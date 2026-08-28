"""Bootstrap the default memory skeleton when an actor has no files yet."""

from __future__ import annotations

import logging
from typing import Any

from dreamer.data_types import Scope
from dreamer.protocols import MemoryFileStore

log = logging.getLogger(__name__)


_DEFAULT_FILES: list[dict[str, Any]] = [
    {
        # Single top-level profile file. NOT under /memories/profile/<name>.md —
        # identity is one document, kept appendable.
        "path": "/memories/profile.md",
        "content": "",
        "description": "Identity, role, team, background, territory, scope",
        "startup_load": True,
    },
    {
        "path": "/memories/preferences/coding.md",
        "content": "",
        "description": "Coding language and tool preferences",
        "startup_load": False,
    },
    {
        "path": "/memories/preferences/communication.md",
        "content": "",
        "description": "Communication style and formatting preferences",
        "startup_load": False,
    },
]


async def bootstrap_default_skeleton(store: MemoryFileStore, scope: Scope) -> list[str]:
    """Create the default files if absent. Returns paths created."""
    created: list[str] = []
    for spec in _DEFAULT_FILES:
        path = spec["path"]
        if await store.file_exists(scope=scope, path=path):
            continue
        await store.add_file(
            scope=scope,
            path=path,
            content=spec["content"],
            description=spec["description"],
            startup_load=spec["startup_load"],
            extra_metadata={"bootstrap": True},
        )
        created.append(path)
    if created:
        log.info("bootstrap: created %d default files for scope %s", len(created), scope)
    return created
