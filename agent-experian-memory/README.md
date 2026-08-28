# agent-experian-memory

Experian's access-bot DSPy pipeline refactored onto the new Databricks
**session store** and **memory store** APIs (`/api/agents/v1`) — built for the
memory-API bug bash. Deployed over the `agent-experian-test` app in the
eng-ml-inference staging workspace.

## What changed vs. Experian's app

Experian's original app is stateless per request: the client resends the whole
conversation every turn. This refactor keeps their deterministic DSPy control
flow (query rewrite → retrieve → generate) but moves conversation state and
long-term memory server-side:

```
predict_stream()
  ├─ memory_manager.before_turn()          # BEFORE the turn
  │    ├─ memory_store.search(actor_id)    #   1. long-term memory for the user
  │    └─ session.list_items()             #   2. last N conversation turns
  ├─ run_pipeline()                        # deterministic DSPy control flow
  │    ├─ query_rewriter.acall(...)
  │    ├─ retrieve_context(...)            # keyword KB stub (no VS in staging)
  │    ├─ answer_generator.acall(..., long_term_memory=...)
  │    └─ memories = memory_extractor(...) # LLM extracts durable user facts
  finally:
  └─ memory_manager.after_turn()           # AFTER the turn
       ├─ session.append(user, assistant)  #   1. append turn to session store
       └─ memory_store.add(each fact)      #   2. write extracted memories
```

Clients send only the newest user message plus `custom_inputs.actor_id`
(scopes memory) and optionally `custom_inputs.session_id` (omit on the first
turn; reuse the value from `custom_outputs.session_id` afterwards).

Retrieval uses a deterministic keyword scorer over the EXPERIAN_PRODUCTS
catalog — staging has no Ascend gateway, guardrails, vector search, or
reranker, so those access-bot stages are stubbed/omitted.

## Stores & routing

| Surface | Store | Routing |
|---|---|---|
| Session store | `experian-bugbash-sessions` | liteswap: `x-databricks-traffic-id: testenv://liteswap/test-shaotong` |
| Memory store | `experian-bugbash-memory` | normal staging gateway |

The vendored `databricks_agent_client/` package (from the bug-bash notebooks)
wraps both APIs.

## Debug console (the app landing page)

Opening the app URL serves an interactive debug console:

- **Chat** — sends only the newest message to `POST /invocations` (streamed);
  the session id round-trips automatically.
- **Under the hood** — after every turn, the exact session/memory REST calls
  it triggered (method, path, request, response, latency) render per phase
  (before_turn / after_turn), plus what was recalled / extracted / written.
- **API explorer** — buttons that call the APIs directly (list sessions,
  list session items, append item, search / list / add / delete memory
  entries), each showing the raw request + response.

Backing endpoints (also curl-able): `GET /debug/traces`, `GET /debug/config`,
`GET /debug/sessions`, `GET /debug/session-items`, `POST /debug/session-append`,
`POST /debug/memory-search`, `GET /debug/memory-entries`,
`POST /debug/memory-add`, `POST /debug/memory-delete`. Registered via
`app.add_api_route` — `include_router` silently no-ops on deployed Apps.

## Auth (important bug-bash caveat)

**No app identity can call the session/memory APIs today**, so the deployed
app uses a 7-day user PAT from secret scope `jenny-bugbash/memory-api-pat`
(env `MEMORY_API_TOKEN`). Locally, the databricks-sdk profile is used (`.env`
sets `DATABRICKS_CONFIG_PROFILE=eng-ml-inference`). Specifics:

- **Service principal**: even with CAN_MANAGE on the backing internal Lakebase
  projects (`databricks-internal-lakebase-agent-{session,memory}-store`), a PG
  role on their production branches, and DML grants on the store database, the
  SP hits a catch-22 — a `USER`-type role fails PG OAuth login
  (`OAuth: User is not authorized`), and a `SERVICE_PRINCIPAL`-type role is
  rejected by the service (*"must be a USER role with LAKEBASE_OAUTH_V1"*).
  Needs a backend fix to accept SERVICE_PRINCIPAL roles.
- **OBO**: forwarded user tokens are downscoped and `/api/agents` demands the
  `all-apis` scope, which is not a valid app `user_api_scope`.

## Run locally

```bash
uv sync && uv run python main.py   # serves on :8000 (DATABRICKS_APP_PORT to override)

curl -X POST localhost:8000/invocations -H "Content-Type: application/json" -d '{
  "input": [{"role": "user", "content": "Is Experian Boost right for me?"}],
  "custom_inputs": {"actor_id": "customer-123"}}'
```

## Deploy

```bash
databricks bundle deploy -p eng-ml-inference
databricks bundle run experian_memory_app -p eng-ml-inference
```

The Apps build installs from `requirements.txt` (regenerate after dependency
changes: `uv export --no-hashes --no-emit-project -o requirements.txt`); the
uv.lock path is rejected by the builder's older uv, and the builder's pip runs
Python 3.11 (hence `requires-python = ">=3.11,<3.13"`).
