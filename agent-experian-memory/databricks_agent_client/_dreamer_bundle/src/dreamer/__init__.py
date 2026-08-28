"""dreamer — offline memory-distillation pipeline.

Per-actor distillation: read an actor's sessions from a Lakebase session
store, extract durable facts with an LLM, and write them as memory entries
into a Lakebase memory store.

Pipeline core (distill -> dedup -> merge -> apply) is store-agnostic behind the
SessionSource and MemoryFileStore protocols; Lakebase adapters live in
``dreamer.lakebase``.
"""

from dreamer.data_types import (
    ChatTurn,
    ConsolidatedFile,
    FileEdit,
    FileEditExisting,
    FileEditNew,
    FileRef,
    IndexContext,
    MergedFile,
    RunReport,
    Scope,
    SessionRef,
)

__all__ = [
    "ChatTurn",
    "ConsolidatedFile",
    "FileEdit",
    "FileEditExisting",
    "FileEditNew",
    "FileRef",
    "IndexContext",
    "MergedFile",
    "RunReport",
    "Scope",
    "SessionRef",
]
