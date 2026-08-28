"""One dreamer run for one actor: settle filtering, bookmark, pipeline call.

This is the piece both entry points share — the ``run_dreamer_job`` notebook
(deployed by the bundle) and ``scripts/run_local.py`` (local dev loop).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from dreamer import pipeline
from dreamer.data_types import RunReport, Scope, SessionRef
from dreamer.lakebase import LakebaseBookmarks, LakebaseMemoryStore, LakebaseSessionSource
from dreamer.llm import DreamerLLM

log = logging.getLogger(__name__)


def _settled_new_sessions(
    sessions: list[SessionRef], *, bookmark: datetime | None, settle_cutoff: datetime
) -> list[SessionRef]:
    """Sessions idle past the settle window and newer than the actor's bookmark.

    The bookmark compares against ``updated_at`` — the pipeline advances it to
    the newest terminally-processed session update time, so anything at or
    below it is already distilled.
    """
    picked = [s for s in sessions if s.updated_at <= settle_cutoff and (bookmark is None or s.updated_at > bookmark)]
    picked.sort(key=lambda s: s.updated_at)
    return picked


async def run_dreamer_for_actor(
    *,
    sessions_conn,
    memory_conn,
    llm: DreamerLLM | None,
    actor_id: str,
    sessions_table: str = "sessions",
    session_items_table: str = "session_items",
    memory_entries_table: str = "memory.memory_entries",
    bookmarks_table: str = "dreamer_bookmarks",
    settle_seconds: int = 0,
    distill_until: datetime | None = None,
    dry_run: bool = False,
    distill_concurrency: int = 4,
    now: datetime | None = None,
    on_progress=None,
) -> RunReport:
    """Distill one actor's settled, not-yet-processed sessions into memory entries.

    The eligible window is (bookmark, cutoff]: the lower bound is the actor's
    stored bookmark (advanced automatically each run), the upper bound the
    smaller of ``now - settle_seconds`` and ``distill_until``.

    ``settle_seconds`` skips sessions updated within the last N seconds (still
    "hot"); 0 distills everything up to now — the right default for a manually
    triggered per-actor run. ``distill_until`` pins the upper bound to an
    absolute timestamp instead (for reproducible/backfill runs).
    """
    now = now or datetime.now(tz=timezone.utc)
    settle_cutoff = datetime.fromtimestamp(now.timestamp() - settle_seconds, tz=timezone.utc)
    if distill_until is not None and distill_until < settle_cutoff:
        settle_cutoff = distill_until

    source = LakebaseSessionSource(sessions_conn, sessions_table=sessions_table, items_table=session_items_table)
    store = LakebaseMemoryStore(memory_conn, entries_table=memory_entries_table)
    bookmarks = LakebaseBookmarks(memory_conn, table=bookmarks_table)
    await bookmarks.ensure_table()

    scope = Scope(user_id=actor_id)
    bookmark_ts = await bookmarks.get(scope=scope)
    all_sessions = await source.list_sessions(user_id=actor_id)
    sessions = _settled_new_sessions(all_sessions, bookmark=bookmark_ts, settle_cutoff=settle_cutoff)
    log.info(
        "actor %s: %d sessions total, %d settled+new (bookmark=%s, settle_cutoff=%s)",
        actor_id,
        len(all_sessions),
        len(sessions),
        bookmark_ts,
        settle_cutoff,
    )
    if not sessions:
        report = RunReport(
            run_id="dreamer:noop",
            scope=scope,
            sessions_found=len(all_sessions),
            bookmark_before=bookmark_ts,
            bookmark_after=bookmark_ts,
            skipped=True,
            skip_reason="no new settled sessions",
        )
        return report

    report = await pipeline.run_user(
        scope=scope,
        sessions=sessions,
        source=source,
        store=store,
        llm=llm,
        bookmark=bookmarks,
        last_processed_at=bookmark_ts,
        distill_concurrency=distill_concurrency,
        dry_run=dry_run,
        on_progress=on_progress,
    )
    report.sessions_found = len(all_sessions)
    report.bookmark_before = bookmark_ts
    # Read back what the pipeline actually persisted (it stops short of any
    # failed session) — this is the watermark the next run starts after.
    report.bookmark_after = bookmark_ts if dry_run else await bookmarks.get(scope=scope)
    return report
