"""Databricks Apps entry point for the Experian memory-API bug-bash agent.

Same @invoke/@stream wrapping as agent-access-bot, but the agent recalls
conversation history from the session store and long-term memory from the
memory store instead of requiring the client to resend prior turns.
"""

import logging
from collections.abc import Generator

import mlflow
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from experian_agent.config import get_config
from experian_agent.responses_agent import ExperianMemoryResponsesAgent

logging.basicConfig(level=logging.INFO)

config = get_config()
if config.mlflow_experiment_id:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=config.mlflow_experiment_id)
mlflow.litellm.autolog()

AGENT = ExperianMemoryResponsesAgent(config)


@invoke()
def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    return AGENT.predict(request)


@stream()
def stream_handler(
    request: ResponsesAgentRequest,
) -> Generator[ResponsesAgentStreamEvent, None, None]:
    yield from AGENT.predict_stream(request)
