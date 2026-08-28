"""Lakebase (Postgres) adapters: session source, memory store, bookmarks.

Expected schemas (the managed session/memory store Postgres layouts):

    sessions(session_id text pk, actor_id text, parent_session_id text,
             root_session_id text, agent_id text, metadata jsonb,
             create_time timestamptz, update_time timestamptz,
             last_activity_time timestamptz)
    session_items(item_id text pk, session_id text fk, position bigint,
                  data jsonb, create_time timestamptz)
    memory_entries(memory_entry_id text pk, actor_id text, session_id text,
                   path text, content text, description text,
                   create_time_ms timestamptz, update_time_ms timestamptz,
                   source_type text)

``session_items.data`` holds one conversation item; message items are parsed
from either the OpenAI-style shape
``{"type": "message", "role": ..., "content": [{"type": "input_text"|"output_text"|"text", "text": ...}]}``
or the plain ``{"role": ..., "content": "..."}`` shape. Non-message items
(tool calls, tool results) are dropped — distillation extracts durable USER
facts, and mechanical tool content (SQL text, search args, JSON results)
dilutes that signal.

Table names are injectable so the same adapters serve the real tables and
``*_j_test`` mock tables. Names are validated against a strict identifier
pattern before being interpolated into SQL.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from dreamer.data_types import ChatTurn, FileRef, IndexContext, Scope, SessionRef

# Matches the managed memory API's SOURCE_TYPE_DREAMER attribution.
SOURCE_TYPE_DREAMER = "SOURCE_TYPE_DREAMER"

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$")


def _table(name: str) -> str:
    """Validate an (optionally schema-qualified) table name for interpolation."""
    if not _IDENT.match(name):
        raise ValueError(f"invalid table name: {name!r}")
    return name


def _ms_to_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


# ── Workspace plumbing: endpoint resolution + connections ──────────────


def resolve_store(workspace_client, store_name: str) -> tuple[str, str, str]:
    """Resolve ``"<project>/<database>"`` to (endpoint_name, endpoint_host, database).

    Looks up the project's read-write compute endpoint through the Lakebase
    Postgres projects API. ``"<project>"`` alone defaults the database to
    ``databricks_postgres``. ``endpoint_name`` is the full resource name
    (``projects/{p}/branches/{b}/endpoints/{e}``) used for credential minting.
    """
    project, _, database = store_name.partition("/")
    database = database or "databricks_postgres"
    resp = workspace_client.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{project}/branches/production/endpoints"
    )
    for ep in resp.get("endpoints", []):
        status = ep.get("status", {})
        if status.get("endpoint_type") == "ENDPOINT_TYPE_READ_WRITE":
            host = status.get("hosts", {}).get("host")
            if host:
                return ep["name"], host, database
    raise RuntimeError(f"no read-write endpoint found for Lakebase project {project!r}")


def pg_password(workspace_client, endpoint_name: str) -> str:
    """Mint a Lakebase database credential (JWT) for one compute endpoint.

    The ambient workspace token is NOT usable as a Postgres password on job
    compute (Lakebase requires a JWT), so mint a real database credential via
    the Postgres credentials API. Called per-connection so it's always fresh.
    """
    resp = workspace_client.api_client.do(
        "POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name}
    )
    return resp["token"]


async def connect_store(workspace_client, store_name: str):
    """Open a psycopg AsyncConnection to ``"<project>/<database>"``.

    Logs in as the caller's identity (user email, or the service principal's
    application id for SP-owned jobs) — that identity must have a Postgres
    role on the project.
    """
    import psycopg

    endpoint_name, host, database = resolve_store(workspace_client, store_name)
    user = workspace_client.current_user.me().user_name
    password = pg_password(workspace_client, endpoint_name)
    return await psycopg.AsyncConnection.connect(
        host=host, port=5432, dbname=database, user=user, password=password, sslmode="require"
    )


# ── Session source ──────────────────────────────────────────────────────


def _item_to_text(data: Any) -> tuple[str | None, str]:
    """Extract (role, text) from one session item's ``data`` payload.

    Returns (None, "") for items that aren't user/assistant messages.
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (ValueError, TypeError):
            return None, ""
    if not isinstance(data, dict):
        return None, ""
    if data.get("type") not in (None, "message"):
        return None, ""
    role = data.get("role")
    if role not in ("user", "assistant"):
        return None, ""
    content = data.get("content")
    if isinstance(content, str):
        return role, content.strip()
    parts: list[str] = []
    if isinstance(content, list):
        for c in content:
            if isinstance(c, str):
                parts.append(c)
            elif isinstance(c, dict) and c.get("type") in ("input_text", "output_text", "text", "summary_text"):
                t = c.get("text") or ""
                if t:
                    parts.append(t)
    return role, "\n".join(parts).strip()


class LakebaseSessionSource:
    """Reads session refs and transcripts from a Lakebase connection.

    ``conn`` is a ``psycopg.AsyncConnection``; the caller owns its lifecycle.
    """

    def __init__(self, conn, *, sessions_table: str = "sessions", items_table: str = "session_items") -> None:
        self._conn = conn
        self._sessions = _table(sessions_table)
        self._items = _table(items_table)

    async def list_sessions(
        self,
        *,
        user_id: str,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> list[SessionRef]:
        query = (
            f"SELECT session_id, actor_id, create_time, update_time"
            f" FROM {self._sessions} WHERE actor_id = %s"
        )
        params: list[object] = [user_id]
        if created_after is not None:
            query += " AND create_time > %s"
            params.append(created_after)
        if created_before is not None:
            query += " AND create_time <= %s"
            params.append(created_before)
        query += " ORDER BY update_time"
        async with self._conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()
        return [
            SessionRef(session_id=r[0], actor_id=r[1], created_at=r[2], updated_at=r[3])
            for r in rows
        ]

    async def load_transcript(self, session_id: str) -> list[ChatTurn]:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"SELECT data, create_time FROM {self._items} WHERE session_id = %s ORDER BY position",
                [session_id],
            )
            rows = await cur.fetchall()
        turns: list[ChatTurn] = []
        for data, create_time in rows:
            role, text = _item_to_text(data)
            if role is None or not text:
                continue
            turns.append(ChatTurn(role=role, content=text, timestamp=create_time))
        return turns


# ── Memory store ────────────────────────────────────────────────────────


class LakebaseMemoryStore:
    """Reads/writes one actor's memory entries through a Lakebase connection.

    The entries table has no uniqueness constraint on (actor_id, path), so
    add/update use check-then-write; a single-actor run has no concurrent
    writers for its scope. ``session_id`` records the most recent contributing
    session (the managed schema holds a single id — known multi-session
    provenance gap).
    """

    def __init__(
        self,
        conn,
        *,
        entries_table: str = "memory.memory_entries",
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        self._conn = conn
        self._entries = _table(entries_table)
        self._now_ms = now_ms

    async def has_any_files(self, *, scope: Scope) -> bool:
        async with self._conn.cursor() as cur:
            await cur.execute(f"SELECT 1 FROM {self._entries} WHERE actor_id = %s LIMIT 1", [scope.user_id])
            return await cur.fetchone() is not None

    async def file_exists(self, *, scope: Scope, path: str) -> bool:
        return await self.view_file(scope=scope, path=path) is not None

    async def view_file(self, *, scope: Scope, path: str) -> tuple[str, dict[str, Any]] | None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"SELECT content, description, session_id FROM {self._entries}"
                " WHERE actor_id = %s AND path = %s ORDER BY update_time_ms DESC LIMIT 1",
                [scope.user_id, path],
            )
            row = await cur.fetchone()
        if row is None:
            return None
        meta: dict[str, Any] = {"description": row[1] or ""}
        if row[2]:
            meta["contributing_session_ids"] = [row[2]]
        return row[0] or "", meta

    async def list_files(self, *, scope: Scope) -> list[FileRef]:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"SELECT path, description, length(coalesce(content, '')), update_time_ms"
                f" FROM {self._entries} WHERE actor_id = %s ORDER BY path",
                [scope.user_id],
            )
            rows = await cur.fetchall()
        # startup_load has no column in the managed schema (documented gap) — always False.
        # update_time_ms is a timestamptz column despite the name; psycopg already returns it
        # as a tz-aware datetime, so it needs no conversion (unlike the raw ms int written above).
        return [
            FileRef(
                path=r[0],
                size=r[2] or 0,
                description=r[1] or "",
                startup_load=False,
                updated_at=r[3],
            )
            for r in rows
        ]

    async def get_index_context(self, *, scope: Scope) -> IndexContext:
        files = await self.list_files(scope=scope)
        lines = [f"- {f.path} — {f.description}" for f in files]
        return IndexContext(
            memory_md="\n".join(lines) if lines else "(no memory files yet)",
            file_paths=[f.path for f in files],
            startup_load_paths=[],
        )

    @staticmethod
    def _last_session_id(extra_metadata: dict[str, Any] | None) -> str | None:
        ids = (extra_metadata or {}).get("contributing_session_ids") or []
        return ids[-1] if ids else None

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
        if not path or path.endswith("/"):
            return False
        if await self.file_exists(scope=scope, path=path):
            return False  # add semantics: an existing row constraint-skips the write
        now = _ms_to_dt(self._now_ms())
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO {self._entries} (memory_entry_id, actor_id, session_id, path,"
                " content, description, source_type, create_time_ms, update_time_ms)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    str(uuid.uuid4()),
                    scope.user_id,
                    self._last_session_id(extra_metadata),
                    path,
                    content,
                    description,
                    SOURCE_TYPE_DREAMER,
                    now,
                    now,
                ],
            )
        await self._conn.commit()
        return True

    async def update_file(
        self,
        *,
        scope: Scope,
        path: str,
        content: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> bool:
        description = (metadata_patch or {}).get("description")
        session_id = self._last_session_id(metadata_patch)
        if content is None and description is None:
            return True  # metadata-only patch with nothing storable is a no-op
        sets = ["update_time_ms = %s"]
        params: list[object] = [_ms_to_dt(self._now_ms())]
        if content is not None:
            sets.append("content = %s")
            params.append(content)
        if description is not None:
            sets.append("description = %s")
            params.append(description)
        if session_id is not None:
            sets.append("session_id = %s")
            params.append(session_id)
        params.extend([scope.user_id, path])
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"UPDATE {self._entries} SET {', '.join(sets)} WHERE actor_id = %s AND path = %s",
                params,
            )
            updated = cur.rowcount >= 1
        await self._conn.commit()
        return updated

    async def delete_file(self, *, scope: Scope, path: str) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"DELETE FROM {self._entries} WHERE actor_id = %s AND path = %s",
                [scope.user_id, path],
            )
        await self._conn.commit()


# ── Bookmarks ───────────────────────────────────────────────────────────


class LakebaseBookmarks:
    """Per-actor incremental watermark, stored next to the memory entries."""

    def __init__(self, conn, *, table: str = "dreamer_bookmarks") -> None:
        self._conn = conn
        self._table = _table(table)

    async def ensure_table(self) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                " actor_id TEXT PRIMARY KEY,"
                " last_processed_at TIMESTAMPTZ,"
                " last_run_id TEXT,"
                " update_time TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
        await self._conn.commit()

    async def get(self, *, scope: Scope) -> datetime | None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                f"SELECT last_processed_at FROM {self._table} WHERE actor_id = %s", [scope.user_id]
            )
            row = await cur.fetchone()
        return row[0] if row else None

    async def set(self, *, scope: Scope, ts: datetime, run_id: str) -> None:
        async with self._conn.cursor() as cur:
            # GREATEST + COALESCE: advance monotonically; a NULL existing value
            # must not swallow the first advance (GREATEST(NULL, ts) is NULL).
            await cur.execute(
                f"INSERT INTO {self._table} (actor_id, last_processed_at, last_run_id, update_time)"
                " VALUES (%s, %s, %s, now())"
                " ON CONFLICT (actor_id) DO UPDATE SET"
                f" last_processed_at = GREATEST(COALESCE({self._table.split('.')[-1]}.last_processed_at,"
                " EXCLUDED.last_processed_at), EXCLUDED.last_processed_at),"
                " last_run_id = EXCLUDED.last_run_id,"
                " update_time = now()",
                [scope.user_id, ts, run_id],
            )
        await self._conn.commit()
