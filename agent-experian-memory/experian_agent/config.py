import logging
from pathlib import Path

from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class ExperianAgentConfiguration(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    # Model env vars may be injected by Databricks Apps as bare serving-endpoint
    # names (app.yaml valueFrom resource references); dspy.LM needs the litellm
    # "databricks/<endpoint>" form.
    @field_validator(
        "query_rewriter_model",
        "answer_generator_model",
        "memory_extractor_model",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def _normalize_model(cls, v):
        if isinstance(v, str) and v and "/" not in v:
            return f"databricks/{v}"
        return v

    # Models (same endpoints Experian's access-bot uses, available in staging).
    # dspy.LM resolves auth through the databricks-sdk default credential chain
    # (profile locally, injected service-principal env on Databricks Apps).
    # Assignment matches Experian's deployed access-bot (agent-memory-store-async):
    # the rewriter/task LM is gemini-3-flash, the answer generator is flash-lite.
    query_rewriter_model: str = "databricks/databricks-gemini-3-flash"
    answer_generator_model: str = "databricks/databricks-gemini-3-1-flash-lite"
    memory_extractor_model: str = "databricks/databricks-gemini-3-1-flash-lite"

    # Session store (short-term / conversation memory). The original
    # experian-bugbash-sessions store's backing database went bad server-side
    # (every op 500s for every identity) — replaced 2026-08-28.
    session_store_name: str = "experian-dspy-sessions"
    # The session-store handlers are live on the normal staging gateway now;
    # set to "testenv://liteswap/<unit>" to route to a LiteSwap unit instead.
    session_store_traffic_id: str = ""
    # "Fetch last N conversation turns" — one turn = user + assistant item.
    history_max_turns: int = 10

    # Memory store (long-term / user-scoped memory)
    memory_store_display_name: str = "experian-dspy-memory"
    memory_store_traffic_id: str = ""  # entries API is live on the normal staging gateway
    memory_search_limit: int = 5

    # Set false to skip the extra LLM call that extracts durable memories
    enable_memory_extraction: bool = True

    # Escape hatch: a user PAT for the /api/agents session+memory APIs.
    # Normally unset — the app SP works once it has CAN_MANAGE + a Postgres
    # role on the backing internal Lakebase projects (see app.yaml header).
    memory_api_token: str | None = None

    # Observability (optional)
    mlflow_experiment_id: str | None = None


_config: ExperianAgentConfiguration | None = None


def get_config(env_path: str | Path | None = None) -> ExperianAgentConfiguration:
    global _config
    if _config is None:
        env_file = Path(env_path) if env_path else Path(__file__).parent.parent / ".env"
        if env_file.exists():
            logger.info("Loading configuration from .env file: %s", env_file.absolute())
            load_dotenv(env_file, override=True)
        _config = ExperianAgentConfiguration(_env_ignore_empty=True)
    return _config
