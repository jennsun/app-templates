# Repro: session/memory store APIs reject Databricks Apps service principals

**Bug bash finding, 2026-08-18 · eng-ml-inference staging (workspace 1653573648247579)**

## TL;DR

A Databricks App's service principal cannot call the `/api/agents/v1` session-store or
memory-store data planes **with any role configuration**, even after every grant we could
find. The Lakebase role's `identity_type` is a catch-22:

| Role `identity_type` | Result of the same GET as the SP |
|---|---|
| `USER` | **500** `Failed to access memory entries: ERROR: OAuth: User is not authorized` (PG OAuth login rejects the SP token) |
| `SERVICE_PRINCIPAL` | **400** `Lakebase Postgres role <sp-client-id> already exists but is not a USER role with LAKEBASE_OAUTH_V1 authentication` (service precondition rejects the role) |
| — (human user token) | **200**, entries returned |

OBO is also impossible (see below), so **no identity available to a deployed app works**.
The backend needs to accept `SERVICE_PRINCIPAL`-type roles (or map SP OAuth subjects onto
the existing role check) before apps can integrate these APIs.

## Environment

- App: `agent-experian-test` → https://agent-experian-test-1653573648247579.staging.aws.databricksapps.com
- App SP: `c6962c7a-2eca-41bd-94ec-e3a76ca586e1` (display name `app-142hnu agent-experian-test`)
- Memory store: `experian-bugbash-memory` (`memory-stores/6eebbb3d-451b-40e4-8024-fc040864cd59`), normal gateway
- Session store: `experian-bugbash-sessions`, LiteSwap unit `test-shaotong` (`x-databricks-traffic-id: testenv://liteswap/test-shaotong`)
- Backing Lakebase projects: `projects/databricks-internal-lakebase-agent-session-store` (uid `3401b6a1-2c9a-414f-97a9-357d067c635a`), `projects/databricks-internal-lakebase-agent-memory-store`

## Grants in place before the repro (i.e. this is not a missing-grant problem)

1. **Project ACL** — `CAN_MANAGE` for the SP on both projects:
   ```
   PATCH /api/2.0/permissions/database-projects/databricks-internal-lakebase-agent-memory-store
   {"access_control_list":[{"service_principal_name":"c6962c7a-2eca-41bd-94ec-e3a76ca586e1","permission_level":"CAN_MANAGE"}]}
   ```
   (note: this permissions endpoint 404s with the project **uuid**; it only accepts the string project id)
2. **Branch-scoped Postgres role** for the SP on both `branches/production`:
   ```python
   w.postgres.create_role(
       parent="projects/databricks-internal-lakebase-agent-memory-store/branches/production",
       role=Role(spec=RoleRoleSpec(
           postgres_role="c6962c7a-2eca-41bd-94ec-e3a76ca586e1",
           identity_type=RoleIdentityType.USER,          # or SERVICE_PRINCIPAL — both fail, see below
           auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1)),
       role_id="c6962c7a-2eca-41bd-94ec-e3a76ca586e1")
   ```
   Confirmed present with `rolcanlogin=true` in `pg_roles`.
3. **Database-level DML** on the session store's database (each store = one Postgres database on
   the branch, owned by its creator; the service connects **as the caller**): `GRANT CONNECT`,
   `USAGE/CREATE` on schema `public`, `SELECT/INSERT/UPDATE/DELETE` on all tables + default
   privileges, granted by the store owner via psycopg.

## Repro A — role `identity_type=USER`: PG OAuth rejects the SP

Request (as the app SP, via its injected M2M OAuth credentials):

```
GET /api/agents/v1/memory-stores/6eebbb3d-451b-40e4-8024-fc040864cd59/entries?actor_id=customer-jenny-test
Authorization: Bearer <app-SP OAuth token>
```

Response:

```
HTTP 500
InternalError: Failed to access memory entries: ERROR: OAuth: User is not authorized
```

## Repro B — role `identity_type=SERVICE_PRINCIPAL`: service rejects the role

Same request, after recreating the role with `identity_type=SERVICE_PRINCIPAL` (delete_role +
create_role, same `postgres_role` and `LAKEBASE_OAUTH_V1`):

```
HTTP 400
BadRequest: Lakebase Postgres role c6962c7a-2eca-41bd-94ec-e3a76ca586e1 already exists
but is not a USER role with LAKEBASE_OAUTH_V1 authentication
```

## Baseline — same request as a human user

```
GET /api/agents/v1/memory-stores/6eebbb3d-451b-40e4-8024-fc040864cd59/entries?actor_id=customer-jenny-test
Authorization: Bearer <user PAT>
→ HTTP 200, {"managed_memory_entries":[ ...11 entries... ]}
```

## Same failure on the session-store API — but masked (secondary bug)

As the SP (role `USER`), with the LiteSwap routing header:

```
GET /api/agents/v1/session-stores/experian-bugbash-sessions/sessions
→ HTTP 500  InternalError: Failed to list sessions

GET /api/agents/v1/session-stores/experian-bugbash-sessions/sessions/sess-1b134c427002
→ HTTP 500  InternalError: Failed to get session

POST /api/agents/v1/session-stores (create store as SP, pre-project-grant)
→ "not authorized ... please assign 'Can Manage' for Database project 3401b6a1-..."
POST /api/agents/v1/session-stores (create store as SP, post-grant)
→ HTTP 500  InternalError: Failed to initialize Lakebase schema: ERROR: OAuth: User is not authorized
```

Same requests as a human user → 200. The session-store service wraps every underlying
cause in `InternalError: Failed to <op>`; the memory-store service surfaces the real error.
Please align the session-store service on the memory-store behavior — the opaque form cost
us hours of diagnosis.

## Third dead end — OBO from Apps

- Calling with the app-forwarded user token (`x-forwarded-access-token`):
  `403 Invalid scope, required scopes: all-apis`
- Granting the scope is impossible: `PATCH /api/2.0/apps/<app>` with
  `user_api_scopes:["all-apis"]` → `INVALID_PARAMETER_VALUE: The specified scope all-apis is
  not a valid scope` (and DAB silently drops it from `databricks.yml`).

## Live one-click repro

The deployed app proxies these calls server-side **as its own SP** when you pass `identity=sp`
(default identity is a user PAT, which succeeds — instant side-by-side):

```bash
T=$(databricks auth token --host https://eng-ml-inference.staging.cloud.databricks.com | jq -r .access_token)
APP=https://agent-experian-test-1653573648247579.staging.aws.databricksapps.com

curl -s -H "Authorization: Bearer $T" "$APP/debug/memory-entries?actor_id=customer-jenny-test"              # 200 as user
curl -s -H "Authorization: Bearer $T" "$APP/debug/memory-entries?actor_id=customer-jenny-test&identity=sp"  # fails as SP
curl -s -H "Authorization: Bearer $T" "$APP/debug/sessions?actor_id=customer-jenny-test&identity=sp"        # fails as SP
```

Or open the app UI → **API explorer** → switch the identity dropdown to *service principal*
and click any button; the raw request/response renders inline.

## Asks for the backend team

1. **Accept service principals in the Lakebase OAuth login path** for `/api/agents` data
   planes — either honor `SERVICE_PRINCIPAL`-type roles in the service's role check, or map
   SP token subjects onto the `USER`-type role it expects. (This is the blocker.)
2. **Un-mask session-store errors** — return the underlying cause like the memory-store
   service does, instead of `InternalError: Failed to <op>`.
3. **Auto-provision the role/grants** the way it happens for human users — SPs shouldn't
   need manual `create_role` + SQL grants per store database. Consider store-level sharing
   semantics too: a store created by user A is a database owned by A's PG role, so every
   other principal needs manual SQL grants today.
4. **A fine-grained OAuth scope for `/api/agents`** so OBO from Apps becomes viable
   (`all-apis` can never be granted to app user tokens).
