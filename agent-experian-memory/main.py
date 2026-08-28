"""Databricks Apps launcher.

Serves the agent on the port Databricks Apps expects (DATABRICKS_APP_PORT,
default 8000). Locally you can run `python main.py`.
"""

import os

import uvicorn

from agent_server.start_server import app


def main() -> None:
    port = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
