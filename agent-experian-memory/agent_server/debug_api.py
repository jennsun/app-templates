"""Debug endpoints for the bug-bash UI: inspect and directly trigger the
session/memory APIs, and read the per-turn API trace.

Every endpoint returns {"api_calls": [...]} — the exact REST calls made
(method, path, query, body, response, latency) — so the UI can show what
happens under the hood, whether triggered by a chat turn or a button click.

NOTE: routes are registered with app.add_api_route(), NOT APIRouter +
include_router — include_router silently no-ops on the deployed Databricks
Apps build of the mlflow AgentServer app.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from databricks_agent_client.memory_store import MemoryStoreClient
from databricks_agent_client.session_store import SessionStoreClient
from experian_agent.memory import (
    RECENT_TURNS,
    TurnMemoryManager,
    TurnTrace,
    build_agent_client,
)

logger = logging.getLogger(__name__)


def register_debug_routes(app: FastAPI, memory_manager: TurnMemoryManager) -> None:
    config = memory_manager.config

    def _json(payload, status: int = 200) -> JSONResponse:
        return JSONResponse(payload, status_code=status)

    def _identity(request: Request, body: dict | None = None) -> str:
        """Which identity to call the APIs with.

        "sp" forces the databricks-sdk default chain (the app service
        principal when deployed) even when MEMORY_API_TOKEN is configured —
        this is how the bug-bash SP failures are reproduced on demand.
        Default: whatever the app is configured with (PAT if set, else SP).
        """
        value = request.query_params.get("identity") or (body or {}).get("identity") or ""
        return "sp" if value == "sp" else "default"

    def _session_client(trace: TurnTrace, identity: str) -> SessionStoreClient:
        token = None if identity == "sp" else config.memory_api_token
        return build_agent_client(config, token, trace=trace).session_store

    def _memory_client(trace: TurnTrace, identity: str) -> MemoryStoreClient:
        token = None if identity == "sp" else config.memory_api_token
        return build_agent_client(config, token, trace=trace).memory_store

    def _run(operation, identity: str = "default") -> JSONResponse:
        """Run `operation(trace)` and return its API-call trace (+ any error)."""
        trace = TurnTrace()
        trace.phase = "explorer"
        error = None
        try:
            operation(trace)
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
        return _json(
            {"identity": identity, "error": error, "api_calls": trace.calls},
            200 if error is None else 502,
        )

    # ------------------------------------------------------------------
    def get_config() -> JSONResponse:
        return _json(
            {
                "session_store": config.session_store_name,
                "session_store_traffic_id": config.session_store_traffic_id or None,
                "memory_store": config.memory_store_display_name,
                "memory_store_traffic_id": config.memory_store_traffic_id or None,
                "history_max_turns": config.history_max_turns,
                "memory_search_limit": config.memory_search_limit,
                "models": {
                    "query_rewriter": config.query_rewriter_model,
                    "answer_generator": config.answer_generator_model,
                    "memory_extractor": config.memory_extractor_model,
                },
            }
        )

    def get_traces() -> JSONResponse:
        return _json({"turns": list(RECENT_TURNS)})

    # ------------------------------------------------------------------
    async def list_sessions(request: Request) -> JSONResponse:
        actor_id = request.query_params.get("actor_id") or None
        identity = _identity(request)

        def op(trace):
            client = _session_client(trace, identity)
            client.list_sessions(
                session_store_name=config.session_store_name,
                page_size=50,
                filter=f"actor_id = '{actor_id}'" if actor_id else None,
            )

        return _run(op, identity)

    async def list_session_items(request: Request) -> JSONResponse:
        session_id = request.query_params.get("session_id")
        identity = _identity(request)
        if not session_id:
            return _json({"error": "session_id query param is required"}, 400)

        def op(trace):
            client = _session_client(trace, identity)
            session = client.get_session(
                session_store_name=config.session_store_name, session_id=session_id
            )
            session.list_items(order_by="create_time asc")

        return _run(op, identity)

    async def append_session_item(request: Request) -> JSONResponse:
        body = await request.json()
        session_id = body.get("session_id")
        identity = _identity(request, body)
        if not session_id:
            return _json({"error": "session_id is required"}, 400)

        def op(trace):
            client = _session_client(trace, identity)
            session = client.get_session(
                session_store_name=config.session_store_name, session_id=session_id
            )
            session.append(
                [
                    {
                        "type": "message",
                        "role": body.get("role", "user"),
                        "content": body.get("content", "(debug append)"),
                    }
                ]
            )

        return _run(op, identity)

    # ------------------------------------------------------------------
    async def search_memory(request: Request) -> JSONResponse:
        body = await request.json()
        identity = _identity(request, body)
        if not body.get("actor_id") or not body.get("query"):
            return _json({"error": "actor_id and query are required"}, 400)

        def op(trace):
            store = memory_manager._resolve_memory_store(_memory_client(trace, identity))
            store.search(
                actor_id=body["actor_id"],
                query=body["query"],
                limit=int(body.get("limit") or config.memory_search_limit),
            )

        return _run(op, identity)

    async def list_memory_entries(request: Request) -> JSONResponse:
        actor_id = request.query_params.get("actor_id")
        identity = _identity(request)
        if not actor_id:
            return _json({"error": "actor_id query param is required"}, 400)

        def op(trace):
            store = memory_manager._resolve_memory_store(_memory_client(trace, identity))
            store.list(actor_id=actor_id)

        return _run(op, identity)

    async def add_memory_entry(request: Request) -> JSONResponse:
        body = await request.json()
        identity = _identity(request, body)
        if not body.get("actor_id") or not body.get("content"):
            return _json({"error": "actor_id and content are required"}, 400)

        def op(trace):
            from uuid import uuid4

            store = memory_manager._resolve_memory_store(_memory_client(trace, identity))
            store.add(
                actor_id=body["actor_id"],
                path=body.get("path") or f"/memories/{uuid4().hex[:12]}.md",
                content=body["content"],
                description=body.get("description") or "Added from the debug UI",
            )

        return _run(op, identity)

    async def delete_memory_entry(request: Request) -> JSONResponse:
        body = await request.json()
        identity = _identity(request, body)
        if not body.get("entry_id"):
            return _json({"error": "entry_id is required"}, 400)

        def op(trace):
            store = memory_manager._resolve_memory_store(_memory_client(trace, identity))
            store.delete(entry_id=body["entry_id"])

        return _run(op, identity)

    # ------------------------------------------------------------------
    app.add_api_route("/debug/config", get_config, methods=["GET"])
    app.add_api_route("/debug/traces", get_traces, methods=["GET"])
    app.add_api_route("/debug/sessions", list_sessions, methods=["GET"])
    app.add_api_route("/debug/session-items", list_session_items, methods=["GET"])
    app.add_api_route("/debug/session-append", append_session_item, methods=["POST"])
    app.add_api_route("/debug/memory-search", search_memory, methods=["POST"])
    app.add_api_route("/debug/memory-entries", list_memory_entries, methods=["GET"])
    app.add_api_route("/debug/memory-add", add_memory_entry, methods=["POST"])
    app.add_api_route("/debug/memory-delete", delete_memory_entry, methods=["POST"])
