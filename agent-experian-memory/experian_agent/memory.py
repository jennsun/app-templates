"""Turn-lifecycle integration with the Databricks session + memory APIs (J4).

Before each turn:
  1. Fetch long-term memory for the user (actor)   — memory_store.search()
  2. Fetch the last N conversation turns           — session.list_items()

After each turn:
  1. Append the turn to the conversation store     — session.append()
  2. Write any explicit memories extracted by the pipeline — memory_store.add()

Both API surfaces are bug-bash beta: session-store calls route to a LiteSwap
unit via the `x-databricks-traffic-id` header; memory-store calls hit the
normal staging gateway.

Auth: clients are built PER REQUEST. Priority: explicit bug-bash PAT
(MEMORY_API_TOKEN) > forwarded OBO user token > databricks-sdk default chain.
The app service principal cannot use the session-store API — the backing
internal Lakebase project rejects it — and OBO tokens can't carry the
required all-apis scope, so the deployed app runs on the PAT.

Observability: every REST call to the session/memory APIs is recorded into a
TurnTrace (method, path, query, body, response, latency). Completed turns are
kept in a ring buffer (RECENT_TURNS) that the debug UI reads via /debug/traces.
"""

import json
import logging
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_agent_client import DatabricksAgentClient
from databricks_agent_client.memory_store import MemoryStoreClient
from databricks_agent_client.models import ManagedMemoryEntry, ManagedMemoryStore
from databricks_agent_client.session_store import SessionStoreClient
from databricks_agent_client.session_models import Session, SessionItem

from experian_agent.config import ExperianAgentConfiguration

logger = logging.getLogger(__name__)

# Ring buffer of recent turn records for the debug UI (/debug/traces).
RECENT_TURNS: deque = deque(maxlen=30)
_RECENT_TURNS_LOCK = threading.Lock()

# Rolling per-endpoint latency samples for the debug UI (/debug/latency).
# Keyed by (api, method, normalized path) so every call to the same logical
# endpoint aggregates together regardless of store/session/entry ids.
_API_LATENCY: dict[tuple[str, str, str], dict] = {}
_API_LATENCY_LOCK = threading.Lock()
_LATENCY_SAMPLES_PER_ENDPOINT = 500

_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)


def _normalize_api_path(path: str) -> str:
    """Collapse ids so /debug/latency groups calls by logical endpoint."""
    path = re.sub(r"(session-stores/)[^/:]+", r"\1{store}", path)
    path = re.sub(r"(sessions/)[^/:]+", r"\1{session_id}", path)
    path = re.sub(r"(memory-stores/)[^/:]+", r"\1{store_id}", path)
    path = re.sub(r"(entries/)[^/:]+", r"\1{entry_id}", path)
    return _UUID_RE.sub("{id}", path)


def _record_latency(api: str, method, path, duration_ms: float, error: str | None) -> None:
    key = (api, str(method), _normalize_api_path(str(path)))
    with _API_LATENCY_LOCK:
        rec = _API_LATENCY.get(key)
        if rec is None:
            rec = {"samples": deque(maxlen=_LATENCY_SAMPLES_PER_ENDPOINT), "count": 0, "errors": 0}
            _API_LATENCY[key] = rec
        rec["count"] += 1
        if error:
            rec["errors"] += 1
        rec["samples"].append(duration_ms)


def latency_summary() -> list[dict]:
    """Aggregated latency per (api, method, endpoint), sorted by call count."""
    with _API_LATENCY_LOCK:
        snapshot = [
            (key, rec["count"], rec["errors"], list(rec["samples"]))
            for key, rec in _API_LATENCY.items()
        ]
    rows = []
    for (api, method, path), count, errors, samples in snapshot:
        ordered = sorted(samples)
        pct = lambda p: ordered[min(len(ordered) - 1, int(round(p * (len(ordered) - 1))))]
        rows.append(
            {
                "api": api,
                "method": method,
                "path": path,
                "count": count,
                "errors": errors,
                "avg_ms": round(sum(ordered) / len(ordered), 1),
                "p50_ms": round(pct(0.50), 1),
                "p95_ms": round(pct(0.95), 1),
                "min_ms": round(ordered[0], 1),
                "max_ms": round(ordered[-1], 1),
                "last_ms": round(samples[-1], 1),
                "window": len(ordered),
            }
        )
    rows.sort(key=lambda r: (-r["count"], r["api"], r["path"]))
    return rows


def _clip(obj, limit: int = 6000):
    """JSON-safe copy of obj, stringified + truncated if it's huge."""
    if obj is None:
        return None
    try:
        s = json.dumps(obj, default=str)
    except Exception:
        s = str(obj)
    if len(s) <= limit:
        try:
            return json.loads(s)
        except Exception:
            return s
    return s[:limit] + " …(truncated)"


class TurnTrace:
    """Chronological record of every session/memory REST call in one scope."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.phase: str = "setup"
        self._lock = threading.Lock()

    def record(
        self,
        *,
        api: str,
        method: str,
        path: str,
        query,
        body,
        response,
        error: str | None,
        duration_ms: float,
        traffic_id: str,
    ) -> None:
        with self._lock:
            self.calls.append(
                {
                    "seq": len(self.calls) + 1,
                    "phase": self.phase,
                    "api": api,
                    "method": method,
                    "path": path,
                    "query": _clip(query),
                    "body": _clip(body),
                    "response": _clip(response),
                    "error": error,
                    "duration_ms": round(duration_ms, 1),
                    "traffic_id": traffic_id or None,
                    "time": datetime.now(timezone.utc).isoformat(),
                }
            )


def build_agent_client(
    config: "ExperianAgentConfiguration",
    api_token: str | None,
    trace: TurnTrace | None = None,
) -> DatabricksAgentClient:
    """A DatabricksAgentClient (the SDK facade from the DSPy demo notebook)
    whose underlying WorkspaceClient stamps the LiteSwap routing header on
    every call and records each call into the given TurnTrace.

    Identity: MEMORY_API_TOKEN if set, else the databricks-sdk default chain
    (the app SP on Databricks Apps, the CLI profile locally). Deliberately NOT
    the x-forwarded-access-token OBO token: app-forwarded user tokens are
    downscoped and /api/agents rejects them with "Invalid scope, required
    scopes: all-apis". The SP works once it holds CAN_MANAGE + a Postgres role
    on the backing internal Lakebase projects.
    """
    if api_token:
        ws = WorkspaceClient(token=api_token, auth_type="pat")
    else:
        ws = WorkspaceClient()

    original_do = ws.api_client.do

    def instrumented_do(*args, **kwargs):
        path = str(kwargs.get("path", args[1] if len(args) > 1 else ""))
        if "/session-stores" in path:
            api_label, traffic_id = "session-store", config.session_store_traffic_id
        else:
            api_label, traffic_id = "memory-store", config.memory_store_traffic_id
        if traffic_id:
            headers = dict(kwargs.get("headers") or {})
            headers["x-databricks-traffic-id"] = traffic_id
            kwargs["headers"] = headers
        t0 = time.perf_counter()
        error = None
        response = None
        try:
            response = original_do(*args, **kwargs)
            return response
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise
        finally:
            duration_ms = (time.perf_counter() - t0) * 1000
            method = kwargs.get("method", args[0] if args else "?")
            call_path = kwargs.get("path", args[1] if len(args) > 1 else "?")
            _record_latency(api_label, method, call_path, duration_ms, error)
            if trace is not None:
                trace.record(
                    api=api_label,
                    method=method,
                    path=call_path,
                    query=kwargs.get("query"),
                    body=kwargs.get("body"),
                    response=response,
                    error=error,
                    duration_ms=duration_ms,
                    traffic_id=traffic_id,
                )

    ws.api_client.do = instrumented_do
    return DatabricksAgentClient(workspace_client=ws)


@dataclass
class TurnContext:
    """Everything the pre-turn fetch produced, handed back for post-turn persistence.

    Carries the request-scoped store handles so after_turn reuses the same
    authenticated (and trace-instrumented) clients.
    """

    turn_id: str
    trace: TurnTrace
    session: Session
    memory_store: ManagedMemoryStore
    actor_id: str
    chat_history: str = ""
    long_term_memory: str = ""
    memories_recalled: list[str] = field(default_factory=list)


class TurnMemoryManager:
    def __init__(self, config: ExperianAgentConfiguration) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._session_store_ready = False
        # "memory-stores/<uuid>" resource name, cached after first resolution
        # so later requests skip the display-name listing round-trip.
        self._memory_store_name: str | None = None

    def _agent_client(self, trace: TurnTrace | None = None) -> DatabricksAgentClient:
        return build_agent_client(self.config, self.config.memory_api_token, trace=trace)

    # ------------------------------------------------------------------
    # Store bootstrap (create-if-not-exists, request-scoped credentials)
    # ------------------------------------------------------------------
    def _ensure_session_store(self, client: SessionStoreClient) -> None:
        with self._lock:
            if self._session_store_ready:
                return
            name = self.config.session_store_name
            try:
                client.get(session_store_name=name)
                logger.info("Using existing session store %r", name)
            except NotFound:
                client.create(
                    session_store_name=name,
                    description="Experian memory-API bug bash: conversation turns",
                    metadata={"owner": "agent-experian-test"},
                )
                logger.info("Created session store %r", name)
            self._session_store_ready = True

    def _resolve_memory_store(self, client: MemoryStoreClient) -> ManagedMemoryStore:
        with self._lock:
            cached_name = self._memory_store_name
        if cached_name is not None:
            store = client.get(store_id=cached_name.rsplit("/", 1)[-1])
        else:
            store = client.get(
                display_name=self.config.memory_store_display_name,
                create_if_not_exists=True,
                description="Experian memory-API bug bash: long-term user memory",
            )
            with self._lock:
                self._memory_store_name = store.name
            logger.info(
                "Using memory store %r (id=%s)", store.display_name, store.store_id
            )
        return store

    def _get_or_create_session(
        self, client: SessionStoreClient, actor_id: str, session_id: str | None
    ) -> Session:
        if session_id:
            try:
                return client.get_session(
                    session_store_name=self.config.session_store_name,
                    session_id=session_id,
                )
            except NotFound:
                logger.info("Session %r not found; creating it", session_id)
        return client.create_session(
            session_store_name=self.config.session_store_name,
            actor_id=actor_id,
            session_id=session_id or f"sess-{uuid4().hex[:12]}",
            metadata={"channel": "chat"},
        )

    # ------------------------------------------------------------------
    # Before each turn
    # ------------------------------------------------------------------
    def before_turn(self, actor_id: str, session_id: str | None, query: str) -> TurnContext:
        trace = TurnTrace()
        trace.phase = "before_turn"
        client = self._agent_client(trace)

        self._ensure_session_store(client.session_store)
        memory_store = self._resolve_memory_store(client.memory_store)
        session = self._get_or_create_session(client.session_store, actor_id, session_id)

        t0 = time.perf_counter()
        chat_history = self._fetch_history_transcript(session)
        memories = self._recall_long_term_memory(memory_store, actor_id, query)
        logger.info(
            "before_turn: session=%s history_chars=%d memories=%d (%.2fs)",
            session.session_id,
            len(chat_history),
            len(memories),
            time.perf_counter() - t0,
        )
        return TurnContext(
            turn_id=uuid4().hex[:12],
            trace=trace,
            session=session,
            memory_store=memory_store,
            actor_id=actor_id,
            chat_history=chat_history,
            long_term_memory="\n".join(f"- {m.content}" for m in memories if m.content),
            memories_recalled=[m.content or "" for m in memories],
        )

    def _fetch_history_transcript(self, session: Session) -> str:
        items: list[SessionItem] = session.list_items(order_by="create_time asc")
        # One turn = a user + assistant message pair; keep the last N turns.
        items = items[-2 * self.config.history_max_turns :]
        lines: list[str] = []
        for item in items:
            data = item.data if isinstance(item.data, dict) else {}
            role = str(data.get("role", "")).strip()
            content = str(data.get("content", "")).strip()
            if role in ("user", "assistant") and content:
                lines.append(f"{role.title()}: {content}")
        return "\n\n".join(lines)

    def _recall_long_term_memory(
        self, memory_store: ManagedMemoryStore, actor_id: str, query: str
    ) -> list[ManagedMemoryEntry]:
        """Semantic search merged with a full listing for the actor.

        Search alone misses meta-questions ("what do you remember about me?")
        that don't semantically match any stored fact, so search hits rank
        first and the actor's remaining entries backfill up to 2x the search
        limit.
        """
        recalled: list[ManagedMemoryEntry] = []
        try:
            recalled = memory_store.search(
                actor_id=actor_id, query=query, limit=self.config.memory_search_limit
            )
        except Exception:
            logger.exception("memory_store.search failed; falling back to list only")
        try:
            seen = {e.path for e in recalled}
            cap = 2 * self.config.memory_search_limit
            for entry in memory_store.list(actor_id=actor_id):
                if len(recalled) >= cap:
                    break
                if entry.path not in seen:
                    recalled.append(entry)
                    seen.add(entry.path)
        except Exception:
            logger.exception("memory_store.list failed; using search results only")
        return recalled

    # ------------------------------------------------------------------
    # After each turn
    # ------------------------------------------------------------------
    def after_turn(
        self,
        turn: TurnContext,
        query: str,
        answer: str,
        extracted_memories: list[str],
    ) -> list[str]:
        """Persist the turn; returns paths of memory entries written."""
        turn.trace.phase = "after_turn"
        try:
            turn.session.append(
                [
                    {"type": "message", "role": "user", "content": query},
                    {"type": "message", "role": "assistant", "content": answer},
                ]
            )
        except Exception:
            logger.exception(
                "session.append failed for session %s; turn not persisted",
                turn.session.session_id,
            )

        written: list[str] = []
        # The extractor has no view of the full store, so it happily re-extracts
        # facts it already saved on a previous turn — dedup by exact content.
        try:
            existing = {
                (e.content or "").strip()
                for e in turn.memory_store.list(actor_id=turn.actor_id)
            }
        except Exception:
            logger.exception("memory_store.list failed; skipping dedup")
            existing = set()
        for memory in extracted_memories:
            memory = (memory or "").strip()
            if not memory or memory in existing:
                continue
            path = f"/memories/{uuid4().hex[:12]}.md"
            try:
                entry = turn.memory_store.add(
                    actor_id=turn.actor_id,
                    session_id=turn.session.session_id,
                    path=path,
                    content=memory,
                    description="Extracted from a conversation turn",
                )
                written.append(entry.path)
            except Exception:
                logger.exception("memory_store.add failed for %r", memory[:120])
        logger.info(
            "after_turn: session=%s appended turn, wrote %d memorie(s)",
            turn.session.session_id,
            len(written),
        )
        self._commit_turn_record(
            turn,
            query=query,
            answer=answer,
            extracted=extracted_memories,
            written=written,
            status="complete",
        )
        return written

    def abandon_turn(self, turn: TurnContext, query: str) -> None:
        """Record a turn that never produced an answer (stream aborted/errored)."""
        self._commit_turn_record(
            turn, query=query, answer=None, extracted=[], written=[], status="aborted"
        )

    def _commit_turn_record(
        self,
        turn: TurnContext,
        *,
        query: str,
        answer: str | None,
        extracted: list[str],
        written: list[str],
        status: str,
    ) -> None:
        record = {
            "turn_id": turn.turn_id,
            "time": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "actor_id": turn.actor_id,
            "session_id": turn.session.session_id,
            "query": query,
            "answer": (answer or "")[:2000] or None,
            "chat_history_chars": len(turn.chat_history),
            "long_term_memories_recalled": turn.memories_recalled,
            "memories_extracted": extracted,
            "memories_written": written,
            "api_calls": list(turn.trace.calls),
        }
        with _RECENT_TURNS_LOCK:
            RECENT_TURNS.appendleft(record)
