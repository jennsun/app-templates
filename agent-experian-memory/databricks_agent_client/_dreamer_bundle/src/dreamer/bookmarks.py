"""Bookmark interface — tracks "last processed updated_at" per actor."""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import Protocol

from dreamer.data_types import Scope, SessionRef


def safe_bookmark_cursor(sessions: list[SessionRef], failed_session_ids: Collection[str]) -> datetime | None:
    """Highest ``updated_at`` the bookmark may advance to without skipping a failure.

    Advancing to ``max(updated_at)`` over merely the *successful* sessions is
    unsafe: a session that failed can have an earlier ``updated_at`` than a later
    success, so the cursor would move past it and it would never be retried. The
    safe cursor is the max ``updated_at`` over the **contiguous prefix** of
    ``sessions`` (in the order they were processed) up to — but not including —
    the first failed session. Returns ``None`` when the first session failed (or
    the list is empty), i.e. the cursor must not advance at all.
    """
    failed = set(failed_session_ids)
    cursor: datetime | None = None
    for s in sessions:
        if s.session_id in failed:
            break
        if s.updated_at is not None and (cursor is None or s.updated_at > cursor):
            cursor = s.updated_at
    return cursor


class Bookmark(Protocol):
    async def get(self, *, scope: Scope) -> datetime | None: ...
    async def set(self, *, scope: Scope, ts: datetime, run_id: str) -> None: ...


class NoOpBookmark:
    """No persistence — every run cold-starts (re-distills everything)."""

    async def get(self, *, scope: Scope) -> datetime | None:
        return None

    async def set(self, *, scope: Scope, ts: datetime, run_id: str) -> None:
        return None
