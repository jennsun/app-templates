"""Stage 2a: dedup proposed NEW file paths.

Deterministic-first: any proposed path that matches an existing file maps to
itself (existing files are never renamed). Remaining new paths go through an
LLM canonicalization call.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from dreamer.data_types import IndexContext
from dreamer.llm import DreamerLLM
from dreamer.prompts import DEDUP_SYSTEM

log = logging.getLogger(__name__)


class _DedupResponse(BaseModel):
    mapping: dict[str, str] = Field(default_factory=dict)


async def dedup_paths(
    *,
    proposed_new_paths: list[str],
    index_ctx: IndexContext,
    llm: DreamerLLM | None,
) -> dict[str, str]:
    """Return ``{proposed_path: canonical_path}`` for each input.

    Anchor rule: any proposed path equal to an existing file path is mapped to
    itself unconditionally. The LLM is only asked about the residue.

    If ``llm`` is None or the LLM call fails, residue paths are returned
    mapped to themselves (identity fallback).
    """
    existing = set(index_ctx.file_paths)
    deterministic: dict[str, str] = {}
    residue: list[str] = []
    for p in proposed_new_paths:
        if p in existing:
            deterministic[p] = p
        else:
            residue.append(p)

    if not residue:
        return deterministic
    if llm is None:
        log.info("dedup: stub mode (no LLM) — returning identity for %d residue paths", len(residue))
        return {**deterministic, **{p: p for p in residue}}

    user_payload = _format_user_payload(residue, index_ctx)
    try:
        resp = await llm.structured(
            system=DEDUP_SYSTEM,
            user=user_payload,
            schema=_DedupResponse,
        )
    except Exception:
        log.exception("dedup LLM call failed; returning identity for residue")
        return {**deterministic, **{p: p for p in residue}}

    out = dict(deterministic)
    for orig in residue:
        canon = resp.mapping.get(orig, orig)
        # Anchor rule (belt-and-suspenders): if the LLM tried to map onto an
        # existing path that wasn't in `existing`, we honor it (it IS the
        # canonical we want). But if it tried to MAP AWAY from an existing
        # path, we ignore.
        out[orig] = canon
    # Existing-file paths in the LLM response are passed through unchanged.
    for ep in existing:
        if ep in resp.mapping:
            out[ep] = ep
    return out


def _format_user_payload(residue: list[str], index_ctx: IndexContext) -> str:
    lines = ["# Existing memory files (NEVER rename these)", ""]
    if index_ctx.file_paths:
        for p in index_ctx.file_paths:
            lines.append(f"- {p}")
    else:
        lines.append("(none)")
    lines.append("")
    lines.append("# Proposed new paths to canonicalize")
    lines.append("")
    for p in residue:
        lines.append(f"- {p}")
    return "\n".join(lines)
