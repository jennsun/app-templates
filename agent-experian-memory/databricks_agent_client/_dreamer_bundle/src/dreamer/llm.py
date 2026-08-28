"""Self-contained LLM wrapper for dreamer stages.

Calls a Databricks model-serving endpoint through the workspace's
OpenAI-compatible chat-completions route (``{host}/serving-endpoints``), so the
pipeline has no dependency on any internal client stack. Each stage formats its
own prompt and asks for structured (JSON) output against a Pydantic schema.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

log = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Merge outputs can approach the 16k-char file cap (~4-5k tokens); leave headroom.
_MAX_TOKENS = 8192


class DreamerLLM:
    """Structured-output chat client against a Databricks serving endpoint.

    ``token_provider`` is called per request so long runs survive token
    rotation (in a notebook job, wire it to WorkspaceClient's authenticate()).
    """

    def __init__(
        self,
        *,
        host: str,
        token_provider: Callable[[], str],
        model: str,
        max_concurrent_requests: int = 4,
        timeout_seconds: float = 300.0,
    ) -> None:
        self._url = f"{host.rstrip('/')}/serving-endpoints/chat/completions"
        self._token_provider = token_provider
        self._model = model
        self._sem = asyncio.Semaphore(max_concurrent_requests)
        self._client = httpx.AsyncClient(timeout=timeout_seconds)
        # Cumulative usage across all structured() calls (incl. parse retries).
        self.call_count = 0
        self.input_tokens = 0
        self.output_tokens = 0

    async def aclose(self) -> None:
        await self._client.aclose()

    async def structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[T],
        max_retries: int = 2,
    ) -> T:
        """Generate JSON conforming to ``schema``. Retries on parse failure."""
        messages: list[dict[str, str]] = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        last_err: Exception | None = None
        for attempt in range(max_retries + 1):
            text = await self._chat(messages)
            text = _strip_code_fence(text.strip())
            try:
                payload = json.loads(text)
                return schema.model_validate(payload)
            except (json.JSONDecodeError, ValidationError) as exc:
                last_err = exc
                log.warning(
                    "dreamer LLM JSON parse failed (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                messages = messages + [
                    {"role": "assistant", "content": text},
                    {
                        "role": "user",
                        "content": (
                            "Your previous response was not valid JSON for the requested "
                            "schema. Reply with ONLY the JSON object, no prose, no "
                            "markdown fences."
                        ),
                    },
                ]
        raise RuntimeError(f"dreamer LLM JSON parse failed after retries: {last_err}")

    async def _chat(self, messages: list[dict[str, str]]) -> str:
        async with self._sem:
            resp = await self._client.post(
                self._url,
                headers={"Authorization": f"Bearer {self._token_provider()}"},
                json={
                    "model": self._model,
                    "messages": messages,
                    "max_tokens": _MAX_TOKENS,
                },
            )
        if resp.status_code != 200:
            raise RuntimeError(f"serving endpoint returned {resp.status_code}: {resp.text[:500]}")
        body = resp.json()
        self.call_count += 1
        usage = body.get("usage") or {}
        self.input_tokens += int(usage.get("prompt_tokens") or 0)
        self.output_tokens += int(usage.get("completion_tokens") or 0)
        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError(f"serving endpoint returned no choices: {json.dumps(body)[:500]}")
        content = choices[0].get("message", {}).get("content")
        if isinstance(content, list):
            # Some endpoints return content as a list of typed blocks.
            content = "".join(c.get("text", "") for c in content if isinstance(c, dict))
        return content or ""


def _strip_code_fence(text: str) -> str:
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()
