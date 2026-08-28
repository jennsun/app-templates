"""Stage 1: per-session distillation.

Given one session's transcript and the user's current memory index, ask the
LLM what facts (if any) should be added to existing files or proposed as new
ones. Returns a list of FileEdit objects.

Falls through to "no edits" when ``llm`` is None — useful for walking the
pipeline end-to-end without burning LLM calls.
"""

from __future__ import annotations

import bisect
import logging
from datetime import datetime
from typing import Annotated, Union

from pydantic import BaseModel, Field

from dreamer.data_types import (
    ChatTurn,
    FileEdit,
    FileEditExisting,
    FileEditNew,
    IndexContext,
    SessionRef,
)
from dreamer.llm import DreamerLLM
from dreamer.prompts import DISTILL_SYSTEM
# Per-turn framing overhead (role header + separators) when estimating
# rendered transcript size.
_TURN_OVERHEAD = 12

log = logging.getLogger(__name__)

# Char budget for the historical-context section (already-distilled turns shown
# for reference only). Keeps the NEW section — the sole edit source — from being
# crowded out on a long-running session. Truncation keeps the MOST RECENT
# history (the turns nearest the new messages), which is what new turns refer to.
DEFAULT_HISTORY_CAP_CHARS = 100_000


class _DistillResponse(BaseModel):
    edits: list[Annotated[Union[FileEditNew, FileEditExisting], Field(discriminator="kind")]] = Field(
        default_factory=list
    )


async def distill_session(
    *,
    session: SessionRef,
    transcript: list[ChatTurn],
    index_ctx: IndexContext,
    llm: DreamerLLM | None,
    last_processed_at: datetime | None = None,
    history_cap: int = DEFAULT_HISTORY_CAP_CHARS,
) -> list[FileEdit]:
    if not transcript:
        return []
    historical, new = _partition(transcript, last_processed_at)
    if not new:
        # Every turn was already distilled in a prior run — nothing new to
        # propose. Skip the LLM call rather than re-distill old content (which
        # yields duplicate edits and paraphrase churn on merge).
        log.info(
            "distill: no new turns since last_processed_at for session %s — skipping",
            session.session_id,
        )
        return []
    if llm is None:
        log.info(
            "distill: stub mode (no LLM) for session %s — returning no edits",
            session.session_id,
        )
        return []
    user_payload = _format_user_payload(session, new, historical, index_ctx, history_cap)
    resp = await llm.structured(
        system=DISTILL_SYSTEM,
        user=user_payload,
        schema=_DistillResponse,
    )
    return list(resp.edits)


def _partition(
    transcript: list[ChatTurn],
    last_processed_at: datetime | None,
) -> tuple[list[ChatTurn], list[ChatTurn]]:
    """Split a transcript into (historical, new) at ``last_processed_at``.

    A turn is historical if its timestamp is known and ``<= last_processed_at``;
    everything else (newer, or untimed) is new — so with no watermark, or turns
    lacking timestamps, the whole transcript is treated as new (distill it all).

    Fast path: transcripts are built by walking the session's steps in
    ``created_at`` order, so when every turn is timed they are non-decreasing by
    timestamp and the split point is found with ``bisect`` in O(log n). If any
    turn is untimed (a source that didn't thread ``step_times`` — see the
    run_user guardrail), the ordering assumption doesn't hold, so fall back to
    the O(n) scan that classifies each turn individually.
    """
    if last_processed_at is None:
        return [], list(transcript)
    if all(t.timestamp is not None for t in transcript):
        # Monotonic-by-timestamp: everything at index < i is <= watermark.
        ts = [t.timestamp for t in transcript]
        i = bisect.bisect_right(ts, last_processed_at)
        return transcript[:i], transcript[i:]
    historical: list[ChatTurn] = []
    new: list[ChatTurn] = []
    for t in transcript:
        if t.timestamp is not None and t.timestamp <= last_processed_at:
            historical.append(t)
        else:
            new.append(t)
    return historical, new


def _cap_history_tail(historical: list[ChatTurn], history_cap: int) -> list[ChatTurn]:
    """Keep the most-recent whole turns of history within ``history_cap`` chars.

    Iterates from the end (newest first) so the retained context is the turns
    nearest the new messages — the ones new turns are most likely to reference.
    """
    kept: list[ChatTurn] = []
    used = 0
    for t in reversed(historical):
        # Per-turn framing overhead (the "## {role}" header + blank line).
        # Shared with the transcript builder's cap so both estimate the same
        # rendered shape with one constant.
        cost = len(t.content) + _TURN_OVERHEAD
        if used + cost > history_cap and kept:
            break
        kept.append(t)
        used += cost
    kept.reverse()
    return kept


def _format_user_payload(
    session: SessionRef,
    new: list[ChatTurn],
    historical: list[ChatTurn],
    index_ctx: IndexContext,
    history_cap: int,
) -> str:
    lines: list[str] = []
    lines.append(f"# Actor (the user this memory belongs to): {session.actor_id}")
    lines.append("")
    lines.append("# Current memory index (memory.md)")
    lines.append("")
    lines.append(index_ctx.memory_md)
    lines.append("")
    lines.append(f"# Session {session.session_id} (created_at: {session.created_at.isoformat()})")
    lines.append("")
    # NEW first: the only turns eligible to produce edits, placed up front so
    # the model acts on them rather than the reference history below.
    lines.append("# NEW MESSAGES — distill facts from THESE ONLY")
    lines.append("")
    for t in new:
        lines.append(f"## {t.role}")
        lines.append(t.content)
        lines.append("")
    if historical:
        capped = _cap_history_tail(historical, history_cap)
        lines.append("# EARLIER CONTEXT — already distilled in a prior run; for reference ONLY, do NOT re-propose")
        lines.append("")
        for t in capped:
            lines.append(f"## {t.role}")
            lines.append(t.content)
            lines.append("")
    return "\n".join(lines)
