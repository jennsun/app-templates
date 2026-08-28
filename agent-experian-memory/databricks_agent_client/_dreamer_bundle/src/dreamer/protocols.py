"""SessionSource / MemoryFileStore Protocols — the pipeline's pluggable edges."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from dreamer.data_types import ChatTurn, FileRef, IndexContext, Scope, SessionRef


class SessionSource(Protocol):
    """Pluggable read side for finding and loading session transcripts."""

    async def list_sessions(
        self,
        *,
        user_id: str,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> list[SessionRef]:
        """List sessions for one actor, filtered by created_at range."""
        ...

    async def load_transcript(self, session_id: str) -> list[ChatTurn]:
        """Load user+assistant text messages for one session."""
        ...


class MemoryFileStore(Protocol):
    """Pluggable read/write surface for one actor's memory files."""

    async def has_any_files(self, *, scope: Scope) -> bool:
        """True if the scope has at least one memory file (used to detect cold start)."""
        ...

    async def file_exists(self, *, scope: Scope, path: str) -> bool: ...

    async def view_file(self, *, scope: Scope, path: str) -> tuple[str, dict[str, Any]] | None:
        """Return (content, metadata) or None if the file doesn't exist."""
        ...

    async def list_files(self, *, scope: Scope) -> list[FileRef]: ...

    async def get_index_context(self, *, scope: Scope) -> IndexContext:
        """Snapshot of the memory index + file paths for distill-time context."""
        ...

    async def add_file(
        self,
        *,
        scope: Scope,
        path: str,
        content: str,
        description: str,
        startup_load: bool = False,
        extra_metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Add a file. Returns True if the write landed, False if it was
        constraint-skipped (invalid path, already exists, etc.)."""
        ...

    async def update_file(
        self,
        *,
        scope: Scope,
        path: str,
        content: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> bool:
        """Update a file. Returns True if the write landed (or was a no-op),
        False if it was constraint-skipped."""
        ...

    async def delete_file(self, *, scope: Scope, path: str) -> None:
        """Remove a file. No-op if it doesn't exist."""
        ...
