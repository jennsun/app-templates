"""ResponsesAgent wrapper: DSPy streaming + session/memory turn lifecycle.

predict_stream() implements the J4 integration contract:

  predict_stream()
    ├─ memory_manager.before_turn()   # READ: long-term memory + last N turns
    ├─ run_pipeline()                 # deterministic DSPy control flow
    │    └─ answer_generator(..., long_term_memory=...)
    │    └─ memories = memory_extractor(...)
    finally:
    └─ memory_manager.after_turn()    # PERSIST: append turn + write memories

The client only ever needs to send the latest user message plus
custom_inputs.actor_id / custom_inputs.session_id — prior turns are recalled
from the session store, not resent. Both IDs round-trip via custom_outputs.
"""

import logging
import time
from collections.abc import Generator
from typing import Any, TypedDict
from uuid import NAMESPACE_DNS, uuid3, uuid4

import dspy
import mlflow
from dspy.streaming import StatusMessage, StreamListener, StreamResponse
from mlflow.pyfunc.model import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from experian_agent.config import ExperianAgentConfiguration
from experian_agent.context import RunContext
from experian_agent.memory import TurnMemoryManager
from experian_agent.runner import ExperianMemoryRunner

logger = logging.getLogger(__name__)

DEFAULT_ACTOR_ID = "anonymous"


class AgentState(TypedDict):
    item_id: str | None
    query_ts: str


def normalize_message_text(content: Any) -> str:
    if isinstance(content, str):
        stripped = content.strip()
        if not stripped:
            raise ValueError("Message content must be non-empty.")
        return stripped
    if isinstance(content, (list, tuple)):
        parts = []
        for block in content:
            text = block.get("text") if isinstance(block, dict) else getattr(block, "text", None)
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
        if parts:
            return "\n".join(parts)
    raise ValueError("Message content must include at least one non-empty text block.")


class ExperianMemoryResponsesAgent(ResponsesAgent):
    def __init__(self, config: ExperianAgentConfiguration) -> None:
        self.config = config
        self.runner = ExperianMemoryRunner(config)
        self.memory_manager = TurnMemoryManager(config)

        self._streamified_runner = dspy.streamify(
            self.runner,
            stream_listeners=[
                StreamListener(signature_field_name="answer", allow_reuse=True),
            ],
            is_async_program=True,
            async_streaming=False,
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:  # type: ignore[override]
        final_event = None
        for event in self.predict_stream(request):
            if event.type == "response.output_item.done":
                final_event = event
        if final_event is None:
            raise ValueError("predict_stream did not yield any completed output items.")
        return ResponsesAgentResponse(
            output=[final_event.item],  # type: ignore[attr-defined]
            custom_outputs=final_event.custom_outputs,
        )

    def predict_stream(  # type: ignore[override]
        self,
        request: ResponsesAgentRequest,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        last_message = request.input[-1]
        if last_message.role != "user":  # type: ignore[union-attr]
            raise ValueError("The last message in the input must have role 'user'.")
        query = normalize_message_text(last_message.content)  # type: ignore[union-attr]

        ci = request.custom_inputs or {}
        actor_id = str(ci.get("actor_id") or ci.get("user_id") or DEFAULT_ACTOR_ID)
        session_id = ci.get("session_id")

        # ------------------------------------------------------------------
        # BEFORE the turn: fetch long-term memory + last N conversation turns
        # ------------------------------------------------------------------
        turn = self.memory_manager.before_turn(actor_id, session_id, query)
        context = RunContext(
            chat_history=turn.chat_history,
            long_term_memory=turn.long_term_memory,
        )

        try:
            mlflow.update_current_trace(
                metadata={"mlflow.trace.session": turn.session.session_id, **{
                    k: str(v) for k, v in ci.items()
                }}
            )
        except Exception:
            pass

        state: AgentState = {"item_id": None, "query_ts": str(time.time())}
        answer: str | None = None
        extracted_memories: list[str] = []
        turn_recorded = False

        try:
            output = self._streamified_runner(query=query, context=context)
            for chunk in output:
                if isinstance(chunk, StreamResponse):
                    # Deterministic ID groups all deltas of this LM call
                    delta = self.create_text_delta(
                        delta=chunk.chunk,
                        item_id=str(
                            uuid3(
                                NAMESPACE_DNS,
                                f"{chunk.predict_name}.{chunk.signature_field_name}"
                                f".query_timestamp_{state['query_ts']}",
                            )
                        ),
                    )
                    if state["item_id"] is None:
                        state["item_id"] = delta["item_id"]
                    yield ResponsesAgentStreamEvent(**delta)

                elif isinstance(chunk, StatusMessage):
                    continue  # no tools in this pipeline

                elif isinstance(chunk, dspy.Prediction):
                    answer = chunk.answer
                    extracted_memories = list(getattr(chunk, "memories", []) or [])
                    # ----------------------------------------------------------
                    # AFTER the turn: append the turn + persist extracted
                    # memories. Done before the final event so custom_outputs
                    # (and the /debug/traces record) include the writes.
                    # ----------------------------------------------------------
                    written = self.memory_manager.after_turn(
                        turn, query, answer, extracted_memories
                    )
                    turn_recorded = True
                    yield ResponsesAgentStreamEvent(
                        type="response.output_item.done",
                        item=self.create_text_output_item(
                            text=answer,
                            id=state["item_id"] or str(uuid4()),
                        ),
                        custom_outputs={
                            "actor_id": actor_id,
                            "session_id": turn.session.session_id,
                            "session_store": self.config.session_store_name,
                            "memory_store": self.config.memory_store_display_name,
                            "long_term_memories_recalled": turn.memories_recalled,
                            "memories_extracted": extracted_memories,
                            "memories_written": written,
                            "debug_turn_id": turn.turn_id,
                        },
                    )
        finally:
            # Safety net: if the stream died before the Prediction arrived,
            # still record the turn's API trace for the debug UI.
            if not turn_recorded:
                if answer is not None:
                    self.memory_manager.after_turn(turn, query, answer, extracted_memories)
                else:
                    self.memory_manager.abandon_turn(turn, query)
