from pathlib import Path

from dotenv import load_dotenv

# Load env vars from .env (local development only) before importing the agent —
# configuration is read from environment variables at import time. On
# Databricks Apps there is no .env file; env comes from app.yaml.
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

from fastapi.responses import HTMLResponse  # noqa: E402
from mlflow.genai.agent_server import AgentServer  # noqa: E402

# Importing the agent registers the @invoke/@stream handlers with the server
import agent_server.agent  # noqa: E402,F401
from agent_server.agent import AGENT  # noqa: E402
from agent_server.debug_api import register_debug_routes  # noqa: E402

agent_server = AgentServer("ResponsesAgent")

# Module-level app object so uvicorn can import it by string
app = agent_server.app  # noqa: F841

# Debug console: chat UI + per-turn API trace + direct session/memory API triggers
register_debug_routes(app, AGENT.memory_manager)

_INDEX_HTML = (Path(__file__).parent / "index.html").read_text()


@app.get("/", include_in_schema=False)
def index() -> HTMLResponse:
    """Interactive debug console for the bug bash (chat + API tracing)."""
    return HTMLResponse(_INDEX_HTML)


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")


if __name__ == "__main__":
    main()
