# Databricks notebook source
# MAGIC %md
# MAGIC # Dreamer: per-actor memory distillation
# MAGIC
# MAGIC Distills one actor's sessions from a Lakebase **session store** into a
# MAGIC Lakebase **memory store** using the dreamer pipeline
# MAGIC (distill → dedup → merge → apply).
# MAGIC
# MAGIC Entry point: `run_dreamer_job(session_store_name, memory_store_name, actor_id)`
# MAGIC where a store name is `"<lakebase-project>/<database>"`.
# MAGIC
# MAGIC Auth model: everything (Lakebase Postgres logins + LLM serving calls) runs
# MAGIC as the job's ambient identity via `WorkspaceClient()` — the user for a
# MAGIC user-run job, the service principal for an SP-owned job. That identity
# MAGIC must have a Postgres role on both Lakebase projects.

# COMMAND ----------

# Serverless runtime lacks psycopg/httpx; nest_asyncio guards asyncio.run in
# notebook kernels that already run an event loop.
# MAGIC %pip install "psycopg[binary]" httpx nest-asyncio --quiet

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import asyncio  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("dreamer_diy")

# The bundle syncs this notebook to <root>/notebooks/ and the package to
# <root>/src/ — import the package relative to the notebook's location.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dreamer.lakebase import connect_store  # noqa: E402
from dreamer.llm import DreamerLLM  # noqa: E402
from dreamer.runner import run_dreamer_for_actor  # noqa: E402

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration — edit these variables

# COMMAND ----------

# ═══════════════════ CONFIG — edit these ═══════════════════
# Which actor's sessions to distill (one actor per run):
ACTOR_ID = "bob"

# Stores, as "<lakebase-project>/<database>":
SESSION_STORE_NAME = "conversation-store-prototype/session_store_test"
MEMORY_STORE_NAME = "databricks-internal-memory-lakebase/memory-c2e6fff2"

# Tables (point at the real managed tables by dropping the _j_test suffixes):
SESSIONS_TABLE = "sessions_j_test"
SESSION_ITEMS_TABLE = "session_items_j_test"
MEMORY_ENTRIES_TABLE = "memory.memory_entries_j_test"
BOOKMARKS_TABLE = "dreamer_bookmarks_j_test"

MODEL_ENDPOINT = "databricks-claude-sonnet-5"

# Time window. Lower bound is automatic: the per-actor bookmark (watermark)
# stored in BOOKMARKS_TABLE — each run advances it, so re-runs never distill
# the same sessions. The upper bound is configurable two ways:
SETTLE_SECONDS = 0  # relative: skip sessions updated in the last N seconds (0 = up to now)
DISTILL_UNTIL = ""  # absolute: ISO timestamp, e.g. "2026-08-13T19:00:00Z" ("" = no cap)

DRY_RUN = False
# ════════════════════════════════════════════════════════════


# When run as a JOB (databricks bundle run --params ...), non-empty job
# parameters override the variables above. Interactively the variables always
# win — widget state left over from older notebook versions must not leak in,
# so the override only applies inside a job run.
def _in_job_run() -> bool:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        return ctx.currentRunId().isDefined()
    except Exception:
        return False


def _job_param(name: str, default):
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return default
    value = (value or "").strip()
    return value if value else default


if _in_job_run():
    ACTOR_ID = _job_param("actor_id", ACTOR_ID)
    SESSION_STORE_NAME = _job_param("session_store_name", SESSION_STORE_NAME)
    MEMORY_STORE_NAME = _job_param("memory_store_name", MEMORY_STORE_NAME)
    SESSIONS_TABLE = _job_param("sessions_table", SESSIONS_TABLE)
    SESSION_ITEMS_TABLE = _job_param("session_items_table", SESSION_ITEMS_TABLE)
    MEMORY_ENTRIES_TABLE = _job_param("memory_entries_table", MEMORY_ENTRIES_TABLE)
    BOOKMARKS_TABLE = _job_param("bookmarks_table", BOOKMARKS_TABLE)
    MODEL_ENDPOINT = _job_param("model_endpoint", MODEL_ENDPOINT)
    SETTLE_SECONDS = int(_job_param("settle_seconds", SETTLE_SECONDS))
    DISTILL_UNTIL = _job_param("distill_until", DISTILL_UNTIL)
    DRY_RUN = str(_job_param("dry_run", DRY_RUN)).lower() == "true"

print(f"actor_id             = {ACTOR_ID}")
print(f"session_store_name   = {SESSION_STORE_NAME}")
print(f"memory_store_name    = {MEMORY_STORE_NAME}")
print(f"tables               = {SESSIONS_TABLE}, {SESSION_ITEMS_TABLE}, {MEMORY_ENTRIES_TABLE}, {BOOKMARKS_TABLE}")
print(f"model / settle / dry = {MODEL_ENDPOINT} / {SETTLE_SECONDS}s / {DRY_RUN}")
print(f"distill_until        = {DISTILL_UNTIL or '(no cap — up to now)'}")

# COMMAND ----------


def run_dreamer_job(session_store_name: str, memory_store_name: str, actor_id: str, **overrides):
    """Distill ``actor_id``'s sessions from the session store into the memory store.

    Store names are ``"<lakebase-project>/<database>"``. Optional overrides:
    model_endpoint, sessions_table, session_items_table, memory_entries_table,
    bookmarks_table, settle_seconds, distill_until (ISO timestamp), dry_run.
    """
    from databricks.sdk import WorkspaceClient
    from datetime import datetime, timezone

    distill_until = None
    _du_raw = str(overrides.get("distill_until") or "").strip()
    if _du_raw:
        distill_until = datetime.fromisoformat(_du_raw.replace("Z", "+00:00"))
        if distill_until.tzinfo is None:
            distill_until = distill_until.replace(tzinfo=timezone.utc)

    if not (session_store_name and memory_store_name and actor_id):
        raise ValueError("session_store_name, memory_store_name, and actor_id are all required")
    w = WorkspaceClient()

    def _token() -> str:
        return w.config.authenticate()["Authorization"].removeprefix("Bearer ").strip()

    llm = DreamerLLM(
        host=w.config.host,
        token_provider=_token,
        model=overrides.get("model_endpoint", "databricks-claude-sonnet-5"),
    )

    def _progress(done: int, total: int) -> None:
        print(f"  ⏳ distilled session {done}/{total}")

    async def _run():
        sessions_conn = await connect_store(w, session_store_name)
        memory_conn = await connect_store(w, memory_store_name)
        try:
            return await run_dreamer_for_actor(
                sessions_conn=sessions_conn,
                memory_conn=memory_conn,
                llm=llm,
                actor_id=actor_id,
                sessions_table=overrides.get("sessions_table", "sessions"),
                session_items_table=overrides.get("session_items_table", "session_items"),
                memory_entries_table=overrides.get("memory_entries_table", "memory.memory_entries"),
                bookmarks_table=overrides.get("bookmarks_table", "dreamer_bookmarks"),
                settle_seconds=int(overrides.get("settle_seconds", 0)),
                distill_until=distill_until,
                dry_run=str(overrides.get("dry_run", "false")).lower() == "true",
                on_progress=_progress,
            )
        finally:
            await sessions_conn.close()
            await memory_conn.close()
            await llm.aclose()

    import nest_asyncio

    nest_asyncio.apply()
    report = asyncio.run(_run())
    log.info(
        "dreamer run done: sessions=%d added=%d updated=%d skipped=%d errors=%d (llm calls=%d in=%d out=%d)",
        report.sessions_processed,
        report.files_added,
        report.files_updated,
        report.files_skipped,
        len(report.errors),
        llm.call_count,
        llm.input_tokens,
        llm.output_tokens,
    )
    global LAST_LLM_STATS
    LAST_LLM_STATS = {
        "calls": llm.call_count,
        "input_tokens": llm.input_tokens,
        "output_tokens": llm.output_tokens,
    }
    return report


LAST_LLM_STATS: dict = {}


# COMMAND ----------
# MAGIC %md
# MAGIC ## Run visualization
# MAGIC Stat tiles, the session→entry funnel, and per-session / per-file breakdowns.

# COMMAND ----------


def render_report(report, llm_stats: dict | None = None) -> None:
    """Render the distillation run as an HTML dashboard via displayHTML."""
    import html as _html

    # Palette: ink/chrome tokens + single-hue ordinal blue ramp (funnel stages)
    # and reserved status colors, always paired with a text label.
    INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
    SURFACE, BORDER, GRID = "#fcfcfb", "rgba(11,11,11,0.10)", "#e1e0d9"
    BLUE_RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab"]
    GOOD, SERIOUS, CRITICAL, SERIES_BLUE = "#0ca30c", "#ec835a", "#d03b3b", "#2a78d6"
    FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

    def esc(s):
        return _html.escape(str(s or ""))

    def chip(color: str, label: str) -> str:
        return (
            f'<span style="display:inline-flex;align-items:center;gap:5px;font-size:12px;color:{INK2}">'
            f'<span style="width:8px;height:8px;border-radius:50%;background:{color};display:inline-block"></span>'
            f"{esc(label)}</span>"
        )

    def tile(value, label, caption="") -> str:
        cap = f'<div style="font-size:11px;color:{MUTED};margin-top:2px">{esc(caption)}</div>' if caption else ""
        return (
            f'<div style="flex:1;min-width:130px;padding:14px 16px;background:{SURFACE};'
            f'border:1px solid {BORDER};border-radius:10px">'
            f'<div style="font-size:26px;font-weight:600;color:{INK};line-height:1.1">{esc(value)}</div>'
            f'<div style="font-size:12px;color:{INK2};margin-top:4px">{esc(label)}</div>{cap}</div>'
        )

    distilled_ok = report.sessions_processed - len(report.failed_session_ids)
    files_written = report.files_added + report.files_updated
    edits_total = sum(s.edits_proposed for s in report.session_results)

    def _ts(dt) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "(none — never run)"

    watermark_html = (
        f'<div style="font-size:12.5px;color:{INK2};margin-top:14px;padding:9px 12px;'
        f'background:{GRID}40;border:1px solid {GRID};border-radius:8px">'
        f'<b style="color:{INK}">Watermark</b> — before: {esc(_ts(report.bookmark_before))} '
        f'&nbsp;→&nbsp; after: <b style="color:{INK}">{esc(_ts(report.bookmark_after))}</b>. '
        f"The next run distills only sessions updated after this timestamp (stored per-actor "
        f"in the bookmarks table; no manual carry-over needed)."
        f"</div>"
    )

    if report.skipped:
        displayHTML(  # noqa: F821
            f'<div style="font-family:{FONT};background:{SURFACE};border:1px solid {BORDER};'
            f'border-radius:12px;padding:20px;max-width:860px;color:{INK}">'
            f'<div style="font-size:16px;font-weight:600">Nothing to distill</div>'
            f'<div style="font-size:13px;color:{INK2};margin-top:6px">Actor <b>{esc(report.scope.user_id)}</b>: '
            f"{report.sessions_found} session(s) in the store, none new + settled "
            f"(skip reason: {esc(report.skip_reason)}).</div>"
            f"{watermark_html}</div>"
        )
        return

    # ── Funnel: sessions → distilled → memory files (single-hue ordinal ramp) ──
    stages = [
        ("Sessions for actor", report.sessions_found or report.sessions_processed),
        ("New + settled (this run)", report.sessions_processed),
        ("Distilled OK", distilled_ok),
        ("Memory files written", files_written),
    ]
    fmax = max(1, max(v for _, v in stages))
    funnel_rows = []
    for (label, value), color in zip(stages, BLUE_RAMP):
        width = max(1.5, 100.0 * value / fmax)
        funnel_rows.append(
            f'<div style="display:flex;align-items:center;gap:10px;margin:7px 0" title="{esc(label)}: {value}">'
            f'<div style="width:190px;font-size:12px;color:{INK2};text-align:right">{esc(label)}</div>'
            f'<div style="flex:1;background:transparent">'
            f'<div style="width:{width:.1f}%;height:20px;background:{color};border-radius:0 4px 4px 0"></div></div>'
            f'<div style="width:40px;font-size:13px;color:{INK};font-variant-numeric:tabular-nums">{value}</div>'
            f"</div>"
        )

    # ── Per-session table ──
    sess_rows = []
    for s in report.session_results:
        status = chip(CRITICAL, "failed") if s.failed else chip(GOOD, "ok")
        sess_rows.append(
            f'<tr style="border-top:1px solid {GRID}">'
            f'<td style="padding:6px 10px;font-family:monospace;font-size:12px;color:{INK}">{esc(s.session_id)}</td>'
            f'<td style="padding:6px 10px;font-size:12px;color:{INK};text-align:right;font-variant-numeric:tabular-nums">{s.turns}</td>'
            f'<td style="padding:6px 10px;font-size:12px;color:{INK};text-align:right;font-variant-numeric:tabular-nums">{s.edits_proposed}</td>'
            f'<td style="padding:6px 10px">{status}</td></tr>'
        )

    # ── Per-file table: which sessions fed which memory entry ──
    action_chip = {
        "created": chip(GOOD, "created"),
        "updated": chip(SERIES_BLUE, "updated"),
        "skipped": chip(SERIOUS, "skipped"),
        "dry-run": chip(MUTED, "dry-run"),
    }
    file_rows = []
    for f in report.file_results:
        sess_chips = " ".join(
            f'<span style="font-family:monospace;font-size:11px;background:{GRID};color:{INK2};'
            f'padding:1px 6px;border-radius:8px;white-space:nowrap">{esc(sid)}</span>'
            for sid in f.contributing_session_ids
        ) or f'<span style="font-size:12px;color:{MUTED}">—</span>'
        conflict = " " + chip(CRITICAL, "conflict") if f.have_conflict else ""
        file_rows.append(
            f'<tr style="border-top:1px solid {GRID}">'
            f'<td style="padding:6px 10px;font-family:monospace;font-size:12px;color:{INK}">{esc(f.path)}</td>'
            f'<td style="padding:6px 10px;white-space:nowrap">{action_chip.get(f.action, esc(f.action))}{conflict}</td>'
            f'<td style="padding:6px 10px;font-size:12px;color:{INK};text-align:right;font-variant-numeric:tabular-nums">{f.chars}</td>'
            f'<td style="padding:6px 10px">{sess_chips}</td>'
            f'<td style="padding:6px 10px;font-size:12px;color:{INK2}">{esc(f.description)}</td></tr>'
        )

    def thead(*cols) -> str:
        cells = "".join(
            f'<th style="padding:6px 10px;font-size:11px;color:{MUTED};font-weight:500;'
            f'text-align:{align};text-transform:uppercase;letter-spacing:0.04em">{esc(c)}</th>'
            for c, align in cols
        )
        return f"<tr>{cells}</tr>"

    llm_stats = llm_stats or {}
    llm_caption = (
        f"{llm_stats.get('input_tokens', 0):,} in / {llm_stats.get('output_tokens', 0):,} out tokens"
        if llm_stats
        else ""
    )
    errors_html = ""
    if report.errors:
        items = "".join(f'<li style="margin:2px 0">{esc(e)}</li>' for e in report.errors)
        errors_html = (
            f'<div style="margin-top:16px">{chip(CRITICAL, f"{len(report.errors)} error(s)")}'
            f'<ul style="font-size:12px;color:{INK2};margin:6px 0 0 16px;padding:0">{items}</ul></div>'
        )

    displayHTML(  # noqa: F821
        f'<div style="font-family:{FONT};background:{SURFACE};border:1px solid {BORDER};'
        f'border-radius:12px;padding:20px 22px;max-width:980px;color:{INK}">'
        f'<div style="font-size:16px;font-weight:600">Dreamer distillation — actor: {esc(report.scope.user_id)}</div>'
        f'<div style="font-size:11px;color:{MUTED};margin-top:2px">{esc(report.run_id)}'
        f'{" · DRY RUN" if report.dry_run else ""}</div>'
        f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px">'
        f'{tile(f"{distilled_ok} / {report.sessions_found or report.sessions_processed}", "sessions distilled", "of sessions found for actor")}'
        f'{tile(edits_total, "facts extracted", "edits proposed by distillation")}'
        f'{tile(report.files_added, "memory entries created")}'
        f'{tile(report.files_updated, "memory entries updated")}'
        f'{tile(llm_stats.get("calls", "—"), "LLM calls", llm_caption)}'
        f"</div>"
        f'<div style="font-size:13px;font-weight:600;color:{INK};margin:20px 0 4px">Distillation funnel</div>'
        f"{''.join(funnel_rows)}"
        f"{watermark_html}"
        f'<div style="font-size:13px;font-weight:600;color:{INK};margin:20px 0 6px">Sessions distilled</div>'
        f'<table style="border-collapse:collapse;width:100%">'
        f'{thead(("session", "left"), ("turns", "right"), ("edits proposed", "right"), ("status", "left"))}'
        f"{''.join(sess_rows)}</table>"
        f'<div style="font-size:13px;font-weight:600;color:{INK};margin:20px 0 6px">Memory entries written '
        f'<span style="font-weight:400;color:{MUTED}">(with contributing sessions)</span></div>'
        f'<table style="border-collapse:collapse;width:100%">'
        f'{thead(("path", "left"), ("action", "left"), ("chars", "right"), ("from sessions", "left"), ("description", "left"))}'
        f"{''.join(file_rows)}</table>"
        f"{errors_html}</div>"
    )


# COMMAND ----------

# Run the distillation with the CONFIG values from the top of the notebook.
if ACTOR_ID:
    report = run_dreamer_job(
        SESSION_STORE_NAME,
        MEMORY_STORE_NAME,
        ACTOR_ID,
        model_endpoint=MODEL_ENDPOINT,
        sessions_table=SESSIONS_TABLE,
        session_items_table=SESSION_ITEMS_TABLE,
        memory_entries_table=MEMORY_ENTRIES_TABLE,
        bookmarks_table=BOOKMARKS_TABLE,
        settle_seconds=SETTLE_SECONDS,
        distill_until=DISTILL_UNTIL,
        dry_run=DRY_RUN,
    )
    render_report(report, LAST_LLM_STATS)
    print(json.dumps(report.model_dump(mode="json"), indent=2, ensure_ascii=False))
    print(f"\nbookmark_after = {report.bookmark_after and report.bookmark_after.isoformat()}")
    print("(next run distills only sessions updated after this; persisted automatically in the bookmarks table)")
else:
    print("ACTOR_ID is empty — set it in the CONFIG cell at the top.")
