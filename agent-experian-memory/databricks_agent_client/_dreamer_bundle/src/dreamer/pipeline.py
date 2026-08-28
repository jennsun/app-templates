"""Top-level dreamer pipeline orchestration.

Both eval and production drivers call ``run_user``. The difference between
modes is which SessionSource, MemoryFileStore, Bookmark, and LLM
implementations are constructed beforehand.

``run_user`` is one pass over a session batch:

1. Bootstrap default skeleton if the user has no files yet.
2. Stage 1: distill each session in parallel against the current memory index.
3. Stage 2a: dedup proposed NEW paths against existing files + each other.
4. Stage 2b: merge each consolidated file's edits with current content.
5. Stage 3: write merged files. Advance bookmark on success.

Eval mode drivers invoke this twice with manually split session lists
(Phase A / Phase B) to simulate cold start + incremental update.
"""

from __future__ import annotations


import asyncio
import logging
import uuid
from datetime import datetime
from typing import Callable

from dreamer.bookmarks import Bookmark, safe_bookmark_cursor
from dreamer.data_types import (
    ConsolidatedFile,
    FileEdit,
    FileEditNew,
    FileResult,
    MergedFile,
    RunReport,
    Scope,
    SessionRef,
    SessionResult,
)
from dreamer.dedup import dedup_paths
from dreamer.distill import distill_session
from dreamer.llm import DreamerLLM
from dreamer.merge import merge_file
from dreamer.protocols import SessionSource
from dreamer.protocols import MemoryFileStore
from dreamer.bootstrap import bootstrap_default_skeleton

log = logging.getLogger(__name__)


async def run_user(
    *,
    scope: Scope,
    sessions: list[SessionRef],
    source: SessionSource,
    store: MemoryFileStore,
    llm: DreamerLLM | None,
    bookmark: Bookmark | None = None,
    last_processed_at: datetime | None = None,
    strict: bool = False,
    distill_concurrency: int = 4,
    merge_concurrency: int = 4,
    dry_run: bool = False,
    on_progress: "Callable[[int, int], None] | None" = None,
) -> RunReport:
    """Process one batch of sessions for one user.

    ``strict``: if True, raises on any per-session distill failure or per-file
    merge/apply failure. Used by eval mode for Phase A so a fault halts before
    Phase B runs.
    """
    run_id = f"dreamer:{uuid.uuid4()}"
    report = RunReport(run_id=run_id, scope=scope, dry_run=dry_run)
    report.sessions_processed = len(sessions)
    # Stores may accumulate constraint skips + startup_load demotions onto the
    # run report as they write. Duck-typed so the pipeline core stays
    # store-agnostic — stores without the hook are fine.
    _bind = getattr(store, "bind_report", None)
    if callable(_bind):
        _bind(report)
    log.info(
        "dreamer run %s scope=(%s,%s,%s) sessions=%d dry_run=%s",
        run_id,
        scope.user_id,
        scope.workspace_id,
        scope.app_id,
        len(sessions),
        dry_run,
    )

    # Cold start: seed the default skeleton if the user has no files.
    if not dry_run and not await store.has_any_files(scope=scope):
        created = await bootstrap_default_skeleton(store, scope)
        log.info("bootstrap created %d default files: %s", len(created), created)

    if not sessions:
        return report

    index_ctx = await store.get_index_context(scope=scope)

    # Watermark for incremental distillation: turns at or before this time were
    # distilled in a prior run and are shown to the LLM as reference-only
    # context, so only newer turns produce edits. None → distill everything.
    # Prefer the explicit arg (callers that read the bookmark themselves, e.g.
    # the local dreamer route); otherwise fetch it from the bookmark.
    if last_processed_at is None and bookmark is not None:
        last_processed_at = await bookmark.get(scope=scope)

    # ── Stage 1: distill (parallel) ──────────────────────────────────
    distill_sem = asyncio.Semaphore(distill_concurrency)

    distill_done = 0
    # session_id -> {"turns": int, "edits": int} for report.session_results.
    session_stats: dict[str, dict[str, int]] = {}

    # Record transcript-load / distill failures on the report. The bookmark is
    # then advanced only across the contiguous prefix of `sessions` before the
    # first failure (see safe_bookmark_cursor) — a failed session, even one with
    # an earlier updated_at than a later success, must not be skipped.
    async def _one_session(s: SessionRef) -> tuple[SessionRef, list[FileEdit]]:
        nonlocal distill_done
        async with distill_sem:
            try:
                transcript = await source.load_transcript(s.session_id)
            except Exception as exc:
                log.exception("transcript load failed for %s", s.session_id)
                if strict:
                    raise
                report.errors.append(f"transcript_load:{s.session_id}:{exc}")
                report.failed_session_ids.append(s.session_id)
                return s, []
            session_stats[s.session_id] = {"turns": len(transcript), "edits": 0}
            # Guardrail: incremental distillation needs per-turn timestamps to
            # split old turns from new. If a watermark is set but the source
            # returned an entirely untimed transcript, the partition silently
            # degrades to "distill everything" — the exact regression that hits
            # when a SessionSource forgets to thread step timestamps. Warn loudly
            # rather than fail (untimed turns still distill correctly, just not
            # incrementally).
            if last_processed_at is not None and transcript:
                n_untimed = sum(1 for t in transcript if t.timestamp is None)
                if n_untimed == len(transcript):
                    log.warning(
                        "incremental distill requested (last_processed_at=%s) but session %s returned "
                        "%d untimed turns — the new/historical split is disabled for it; the SessionSource "
                        "should pass step_times to step_jsons_to_turns",
                        last_processed_at,
                        s.session_id,
                        len(transcript),
                    )
                elif n_untimed:
                    # Partial regression: some turns threaded a timestamp, some didn't.
                    # The split still runs but silently degrades (untimed turns can't
                    # be placed against the watermark). Lower-severity than the fully-
                    # untimed case, but still worth surfacing so a half-wired source is caught.
                    log.info(
                        "session %s returned %d/%d untimed turns — the new/historical split is "
                        "partial for it; check that the SessionSource threads step_times for every step",
                        s.session_id,
                        n_untimed,
                        len(transcript),
                    )
            try:
                edits = await distill_session(
                    session=s,
                    transcript=transcript,
                    index_ctx=index_ctx,
                    llm=llm,
                    last_processed_at=last_processed_at,
                )
            except Exception as exc:
                log.exception("distill failed for %s", s.session_id)
                if strict:
                    raise
                report.errors.append(f"distill:{s.session_id}:{exc}")
                report.failed_session_ids.append(s.session_id)
                return s, []
            finally:
                distill_done += 1
                if on_progress:
                    on_progress(distill_done, len(sessions))
            session_stats[s.session_id]["edits"] = len(edits)
            return s, edits

    distill_results = await asyncio.gather(*[_one_session(s) for s in sessions])

    failed_set = set(report.failed_session_ids)
    report.session_results = [
        SessionResult(
            session_id=s.session_id,
            turns=session_stats.get(s.session_id, {}).get("turns", 0),
            edits_proposed=session_stats.get(s.session_id, {}).get("edits", 0),
            failed=s.session_id in failed_set,
        )
        for s in sessions
    ]

    # Build matrix M: path -> [edits], path -> [contributing session_ids]
    edits_by_path: dict[str, list[FileEdit]] = {}
    sessions_by_path: dict[str, list[str]] = {}
    proposed_new_paths: list[str] = []
    for s, edits in distill_results:
        for edit in edits:
            if isinstance(edit, FileEditNew):
                proposed_new_paths.append(edit.path)
            edits_by_path.setdefault(edit.path, []).append(edit)
            sessions_by_path.setdefault(edit.path, []).append(s.session_id)

    if not edits_by_path:
        log.info("no edits proposed across %d sessions; nothing to merge", len(sessions))
        await _advance_bookmark(bookmark, scope, sessions, report.failed_session_ids, run_id, dry_run)
        return report

    # ── Stage 2a: dedup ──────────────────────────────────────────────
    canonical_map = await dedup_paths(
        proposed_new_paths=sorted(set(proposed_new_paths)),
        index_ctx=index_ctx,
        llm=llm,
    )

    # Collapse the matrix using the mapping
    existing_paths = set(index_ctx.file_paths)
    consolidated: dict[str, ConsolidatedFile] = {}
    consolidated_sessions: dict[str, list[str]] = {}
    for path, edits in edits_by_path.items():
        canonical = canonical_map.get(path, path)
        is_new = canonical not in existing_paths
        bucket = consolidated.setdefault(
            canonical,
            ConsolidatedFile(path=canonical, edits=[], is_new=is_new),
        )
        bucket.edits.extend(edits)
        consolidated_sessions.setdefault(canonical, []).extend(sessions_by_path.get(path, []))

    # ── Stage 2b: merge (parallel) ───────────────────────────────────
    merge_sem = asyncio.Semaphore(merge_concurrency)

    async def _one_merge(cf: ConsolidatedFile) -> MergedFile | None:
        async with merge_sem:
            try:
                existing = await store.view_file(scope=scope, path=cf.path)
                if existing is not None:
                    current_content, current_meta = existing
                else:
                    current_content, current_meta = None, {}
                current_description = current_meta.get("description") if isinstance(current_meta, dict) else None
                if existing is not None:
                    current_startup_load = (
                        bool(current_meta.get("startup_load", False)) if isinstance(current_meta, dict) else False
                    )
                else:
                    # New file: use the distill LLM's startup_load decision from
                    # the edit (FileEditNew carries startup_load).
                    current_startup_load = any(getattr(e, "startup_load", False) for e in cf.edits)
                # Prior session_ids — what previously contributed to current_content.
                # The merge LLM uses these as the source label for conflict sides
                # whose claim comes from the existing file content.
                prior_session_ids: list[str] = []
                if isinstance(current_meta, dict):
                    raw_prior = current_meta.get("contributing_session_ids") or []
                    if isinstance(raw_prior, list):
                        prior_session_ids = [str(s) for s in raw_prior]
                # Parallel to cf.edits — same order as edits were appended into the bucket.
                session_ids_per_edit = list(consolidated_sessions.get(cf.path, []))
                merged = await merge_file(
                    consolidated=cf,
                    session_ids_per_edit=session_ids_per_edit,
                    current_content=current_content,
                    current_description=current_description,
                    current_startup_load=current_startup_load,
                    prior_session_ids=prior_session_ids,
                    llm=llm,
                )
                # New session_ids from this run; pipeline._apply_one unions
                # them with whatever already lives in the file's metadata.
                merged.contributing_session_ids = list(dict.fromkeys(session_ids_per_edit))
                return merged
            except Exception as exc:
                log.exception("merge failed for %s", cf.path)
                if strict:
                    raise
                report.errors.append(f"merge:{cf.path}:{exc}")
                return None

    merged_files = await asyncio.gather(*[_one_merge(cf) for cf in consolidated.values()])

    # ── Stage 3: apply ───────────────────────────────────────────────
    def _file_result(merged: MergedFile, action: str) -> FileResult:
        return FileResult(
            path=merged.path,
            action=action,
            chars=len(merged.content),
            description=merged.description,
            contributing_session_ids=list(merged.contributing_session_ids),
            have_conflict=merged.have_conflict,
        )

    if dry_run:
        log.info(
            "dry_run: would write %d files",
            sum(1 for m in merged_files if m is not None),
        )
        report.file_results = [_file_result(m, "dry-run") for m in merged_files if m is not None]
    else:
        for cf, merged in zip(consolidated.values(), merged_files):
            if merged is None:
                continue
            try:
                # A store may swallow a constraint rejection internally (recording
                # a ConstraintSkip + bumping files_skipped) and return normally.
                # Detect that via files_skipped so we don't also count the file as
                # added/updated — otherwise one rejected file is double-counted.
                skipped_before = report.files_skipped
                await _apply_one(store, scope, merged, run_id)
                if report.files_skipped > skipped_before:
                    report.file_results.append(_file_result(merged, "skipped"))
                    continue  # store recorded a constraint skip; nothing was written
                if cf.is_new:
                    report.files_added += 1
                    report.file_results.append(_file_result(merged, "created"))
                else:
                    report.files_updated += 1
                    report.file_results.append(_file_result(merged, "updated"))
            except Exception as exc:
                log.exception("apply failed for %s", merged.path)
                if strict:
                    raise
                report.errors.append(f"apply:{merged.path}:{exc}")
                report.files_skipped += 1
                report.file_results.append(_file_result(merged, "skipped"))

    await _advance_bookmark(bookmark, scope, sessions, report.failed_session_ids, run_id, dry_run)
    log.info(
        "dreamer run %s done — sessions=%d added=%d updated=%d skipped=%d errors=%d",
        run_id,
        report.sessions_processed,
        report.files_added,
        report.files_updated,
        report.files_skipped,
        len(report.errors),
    )
    return report


async def _apply_one(
    store: MemoryFileStore,
    scope: Scope,
    merged: MergedFile,
    run_id: str,
) -> None:
    exists = await store.file_exists(scope=scope, path=merged.path)
    # contributing_session_ids accumulates across runs — union new entries with
    # whatever the file already has in its metadata. Same for have_conflict
    # (sticky: once flagged, stays flagged until a future merge resolves).
    prior_session_ids: list[str] = []
    prior_have_conflict = False
    if exists:
        view = await store.view_file(scope=scope, path=merged.path)
        if view is not None:
            _, prior_meta = view
            if isinstance(prior_meta, dict):
                raw = prior_meta.get("contributing_session_ids") or []
                if isinstance(raw, list):
                    prior_session_ids = [str(s) for s in raw]
                prior_have_conflict = bool(prior_meta.get("have_conflict", False))
    all_session_ids = list(dict.fromkeys(prior_session_ids + list(merged.contributing_session_ids)))
    extra_metadata: dict = {
        "last_updating_run_id": run_id,
        "contributing_session_ids": all_session_ids,
        "have_conflict": bool(merged.have_conflict or prior_have_conflict),
    }
    if merged.oversize:
        extra_metadata["oversize"] = True
    if exists:
        await store.update_file(
            scope=scope,
            path=merged.path,
            content=merged.content,
            metadata_patch={"description": merged.description, **extra_metadata},
        )
    else:
        extra_metadata["source_run_id"] = run_id
        await store.add_file(
            scope=scope,
            path=merged.path,
            content=merged.content,
            description=merged.description,
            startup_load=merged.startup_load,
            extra_metadata=extra_metadata,
        )


async def _advance_bookmark(
    bookmark: Bookmark | None,
    scope: Scope,
    sessions: list[SessionRef],
    failed_session_ids: list[str],
    run_id: str,
    dry_run: bool,
) -> None:
    if bookmark is None or not sessions or dry_run:
        return
    # Advance only across the contiguous successful prefix — never past a
    # failed session (see safe_bookmark_cursor).
    cursor = safe_bookmark_cursor(sessions, failed_session_ids)
    if cursor is None:
        return
    await bookmark.set(scope=scope, ts=cursor, run_id=run_id)
