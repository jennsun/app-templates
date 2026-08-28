"""Pydantic models passed between dreamer pipeline stages.

Internal to the dreamer package — adapters at the boundaries translate from
these shapes to/from the session store's items and the memory store's entries.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field


class Scope(BaseModel):
    """Identity tuple a personal-memory file system is keyed by.

    ``user_id`` carries the actor_id — the identity whose sessions are read and
    whose memory entries are written.
    """

    user_id: str
    workspace_id: str = ""
    app_id: str = "dreamer-diy"


class SessionRef(BaseModel):
    """Minimal session metadata the pipeline needs."""

    session_id: str
    actor_id: str
    created_at: datetime
    updated_at: datetime
    ended: bool = False


class ChatTurn(BaseModel):
    """One user or assistant message from a session transcript.

    ``timestamp`` is when the turn occurred (the session item's create_time).
    Used to split a transcript into already-distilled (``<= last_processed_at``)
    vs. new turns so distillation only proposes edits from what's new. Turns
    with no timestamp are treated as new.
    """

    role: Literal["user", "assistant"]
    content: str
    timestamp: datetime | None = None


class FileEditNew(BaseModel):
    """A proposed brand-new memory file from Stage 1 distillation."""

    kind: Literal["new"] = "new"
    path: str
    content: str
    description: str
    startup_load: bool = False
    rationale: str = ""


class FileEditExisting(BaseModel):
    """Edits targeting a memory file that already exists."""

    kind: Literal["existing"] = "existing"
    path: str
    additions: list[str] = Field(default_factory=list)
    rationale: str = ""


FileEdit = Annotated[Union[FileEditNew, FileEditExisting], Field(discriminator="kind")]


class FileRef(BaseModel):
    """Cheap pointer to a stored file — used for index rendering and listings."""

    path: str
    size: int
    description: str
    startup_load: bool
    have_conflict: bool = False
    updated_at: datetime | None = None


class IndexContext(BaseModel):
    """Snapshot of the actor's current memory file system passed into Stage 1."""

    memory_md: str  # rendered index of files + descriptions
    file_paths: list[str]  # all known paths
    startup_load_paths: list[str]


class ConsolidatedFile(BaseModel):
    """One row of the post-dedup matrix — all edits targeting one canonical path."""

    path: str
    edits: list[FileEdit]
    is_new: bool  # path didn't exist before this pipeline run


class MergedFile(BaseModel):
    """Output of Stage 2b — what gets written by Stage 3."""

    path: str
    content: str
    description: str
    startup_load: bool
    oversize: bool = False  # exceeds MAX_CONTENT_LENGTH
    have_conflict: bool = False  # content contains <conflict_begin>...<conflict_end> blocks
    contributing_session_ids: list[str] = Field(default_factory=list)


class ConstraintSkip(BaseModel):
    """One file skipped because a storage constraint rejected the write."""

    path: str
    op: str  # "add" | "update"
    error: dict[str, Any] = Field(default_factory=dict)


class StartupLoadDemotion(BaseModel):
    """One file coerced from ``startup_load=true`` to ``false`` by a budget guard."""

    path: str
    content_chars: int
    budget: int
    running_total: int


class SessionResult(BaseModel):
    """Per-session distillation outcome — feeds run visualizations."""

    session_id: str
    turns: int = 0
    edits_proposed: int = 0
    failed: bool = False


class FileResult(BaseModel):
    """Per-file apply outcome — which sessions fed which memory entry."""

    path: str
    action: Literal["created", "updated", "skipped", "dry-run"]
    chars: int = 0
    description: str = ""
    contributing_session_ids: list[str] = Field(default_factory=list)
    have_conflict: bool = False


class RunReport(BaseModel):
    """Summary of one pipeline run for one actor."""

    run_id: str
    scope: Scope
    # Sessions that exist for the actor in the store (before settle/bookmark
    # filtering). Set by the runner; 0 when the pipeline is called directly.
    sessions_found: int = 0
    sessions_processed: int = 0
    # The actor's watermark around this run (set by the runner): sessions with
    # update_time <= bookmark_before were already distilled before this run;
    # the next run distills only sessions updated after bookmark_after.
    bookmark_before: datetime | None = None
    bookmark_after: datetime | None = None
    files_added: int = 0
    files_updated: int = 0
    files_skipped: int = 0
    errors: list[str] = Field(default_factory=list)
    # Session ids that failed transcript-load or distill this run. The bookmark
    # must not advance past any of these — see safe_bookmark_cursor.
    failed_session_ids: list[str] = Field(default_factory=list)
    constraint_skips: list[ConstraintSkip] = Field(default_factory=list)
    startup_load_demotions: list[StartupLoadDemotion] = Field(default_factory=list)
    session_results: list[SessionResult] = Field(default_factory=list)
    file_results: list[FileResult] = Field(default_factory=list)
    skipped: bool = False
    skip_reason: str | None = None
    dry_run: bool = False
