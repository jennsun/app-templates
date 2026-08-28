"""Stage 2b: per-file merge.

Takes one file's current content + all the edits targeting it and produces
the next version. LLM-driven when an LLM is provided; falls back to a simple
concatenation stub for end-to-end plumbing tests.

Conflict handling: when the LLM detects conflicting claims about the same
underlying fact (across edits or vs. current content), it preserves every
claim inline using ``<conflict_begin>...<conflict_end>`` blocks and sets
``have_conflict=True``. Downstream surfaces this for human resolution.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

# Max chars for one memory file's content — matches the merge prompt's
# "keep the file under 16384 characters" guidance.
MAX_CONTENT_LENGTH = 16384
from dreamer.data_types import (
    ConsolidatedFile,
    FileEditExisting,
    FileEditNew,
    MergedFile,
)
from dreamer.llm import DreamerLLM
from dreamer.prompts import MERGE_SYSTEM

log = logging.getLogger(__name__)


class _MergeResponse(BaseModel):
    content: str
    description: str = ""
    have_conflict: bool = False
    rationale: str = ""


async def merge_file(
    *,
    consolidated: ConsolidatedFile,
    session_ids_per_edit: list[str],  # parallel to consolidated.edits
    current_content: str | None,
    current_description: str | None,
    current_startup_load: bool,
    prior_session_ids: list[str],  # sessions that contributed to current_content
    llm: DreamerLLM | None,
) -> MergedFile:
    """Merge the consolidated edits with current content into a new file body.

    ``session_ids_per_edit`` MUST be the same length as ``consolidated.edits``
    and provides the source attribution the merge prompt uses for labeling
    conflicts. Callers (pipeline.run_user) ensure this invariant.

    ``prior_session_ids`` is the union of session_ids that contributed to
    ``current_content`` across all prior pipeline runs. The merge LLM uses
    this list to label conflict sides whose claim comes from the current
    file content (instead of from a new edit).
    """
    if len(session_ids_per_edit) != len(consolidated.edits):
        raise ValueError(
            f"session_ids_per_edit length {len(session_ids_per_edit)} != "
            f"edits length {len(consolidated.edits)} for {consolidated.path}"
        )
    if llm is None:
        return _stub_merge(consolidated, current_content, current_description, current_startup_load)
    user_payload = _format_user_payload(
        consolidated,
        session_ids_per_edit,
        current_content,
        current_description,
        prior_session_ids,
    )
    try:
        resp = await llm.structured(
            system=MERGE_SYSTEM,
            user=user_payload,
            schema=_MergeResponse,
        )
    except Exception:
        log.exception("merge LLM call failed for %s; falling back to stub", consolidated.path)
        return _stub_merge(consolidated, current_content, current_description, current_startup_load)
    content = resp.content
    description = resp.description.strip() or (current_description or "")
    return MergedFile(
        path=consolidated.path,
        content=content,
        description=description,
        # Never auto-promote: keep existing startup_load. New files default false.
        startup_load=current_startup_load,
        oversize=len(content) > MAX_CONTENT_LENGTH,
        have_conflict=resp.have_conflict,
        contributing_session_ids=[],  # populated by pipeline
    )


def _stub_merge(
    consolidated: ConsolidatedFile,
    current_content: str | None,
    current_description: str | None,
    current_startup_load: bool,
) -> MergedFile:
    parts: list[str] = []
    if current_content:
        parts.append(current_content.rstrip())
    for edit in consolidated.edits:
        if isinstance(edit, FileEditNew):
            if edit.content.strip():
                parts.append(edit.content.rstrip())
        elif isinstance(edit, FileEditExisting):
            for addition in edit.additions:
                if addition.strip():
                    parts.append(addition.rstrip())
    content = "\n\n".join(p for p in parts if p)
    return MergedFile(
        path=consolidated.path,
        content=content,
        description=current_description or "",
        startup_load=current_startup_load,
        oversize=len(content) > MAX_CONTENT_LENGTH,
        have_conflict=False,  # stub never detects conflicts
        contributing_session_ids=[],
    )


def _format_user_payload(
    consolidated: ConsolidatedFile,
    session_ids_per_edit: list[str],
    current_content: str | None,
    current_description: str | None,
    prior_session_ids: list[str],
) -> str:
    lines = ["# File path", consolidated.path, ""]
    lines.append("# Current description")
    lines.append(current_description or "(none)")
    lines.append("")
    lines.append("# Current content")
    prior_label = ", ".join(prior_session_ids) if prior_session_ids else "(none — file is new this run)"
    lines.append(f"prior_session_ids: {prior_label}")
    lines.append("```")
    lines.append(current_content if current_content is not None else "(empty)")
    lines.append("```")
    lines.append("")
    lines.append("# Edits to incorporate")
    for i, (edit, sid) in enumerate(
        zip(consolidated.edits, session_ids_per_edit),
        start=1,
    ):
        if isinstance(edit, FileEditNew):
            lines.append(f"## Edit {i} (new-file proposal)")
            lines.append(f"source: {sid or 'unknown'}")
            if edit.rationale:
                lines.append(f"rationale: {edit.rationale}")
            lines.append("content:")
            lines.append("```")
            lines.append(edit.content)
            lines.append("```")
        elif isinstance(edit, FileEditExisting):
            lines.append(f"## Edit {i} (additions to existing file)")
            lines.append(f"source: {sid or 'unknown'}")
            if edit.rationale:
                lines.append(f"rationale: {edit.rationale}")
            for j, addition in enumerate(edit.additions, start=1):
                lines.append(f"- addition {j}: {addition}")
        lines.append("")
    return "\n".join(lines)
