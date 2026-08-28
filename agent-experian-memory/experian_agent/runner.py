"""Deterministic DSPy pipeline, mirroring Experian's access-bot control flow.

run_pipeline() keeps the same deliberate steps as the original runner:

    ├─ query_rewriter.acall(...)          # decompose the request (LLM)
    ├─ retrieve_context(...)              # plain I/O — keyword-scored product KB
    ├─ answer_generator.acall(..., long_term_memory=...)   # generation uses memory
    └─ memory_extractor.acall(...)        # GENERATE memory content (LLM)

The memory READ (before generate) and PERSIST (after stream) steps live in
TurnMemoryManager and are driven from the ResponsesAgent wrapper, exactly as
sketched for the integration: reads happen before run_pipeline is entered,
writes happen in predict_stream's finally block.

Differences vs. Experian production: no Ascend gateway, guardrails, vector
search, or reranker — those resources don't exist in the staging bug-bash
workspace. Retrieval is a deterministic keyword scorer over the same
EXPERIAN_PRODUCTS catalog their prompts ship with.
"""

import logging
import re

import dspy

from experian_agent.config import ExperianAgentConfiguration
from experian_agent.context import RunContext
from experian_agent.prompts import (
    ANSWER_GENERATION_PROMPT,
    EXPERIAN_PRODUCTS,
    MEMORY_EXTRACTION_PROMPT,
    QUERY_REWRITE_PROMPT,
)
from experian_agent.signatures import (
    AnswerGeneratorSignature,
    MemoryExtractorSignature,
    QueryRewriterSignature,
)

logger = logging.getLogger(__name__)

MAX_DECOMPOSED_QUERIES = 5
PRIORITIZED_TOP_K, OTHER_TOP_K = 1, 2
_STOPWORDS = frozenset(
    "a an and are can do does for how i in is it me my of on or the to what when where with you your".split()
)

dspy.configure_cache(enable_disk_cache=False, enable_memory_cache=True)


class ExperianMemoryRunner(dspy.Module):
    def __init__(self, config: ExperianAgentConfiguration) -> None:
        self.config = config

        self.task_lm = dspy.LM(model=config.query_rewriter_model, temperature=0.0, max_tokens=4096)
        self.main_lm = dspy.LM(
            model=config.answer_generator_model, temperature=0.8, max_tokens=2048
        )
        self.memory_lm = dspy.LM(
            model=config.memory_extractor_model, temperature=0.0, max_tokens=1024
        )

        self.query_rewriter = dspy.Predict(
            QueryRewriterSignature.with_instructions(QUERY_REWRITE_PROMPT)
        )
        self.answer_generator = dspy.ChainOfThought(
            AnswerGeneratorSignature.with_instructions(ANSWER_GENERATION_PROMPT)
        )
        self.memory_extractor = dspy.Predict(
            MemoryExtractorSignature.with_instructions(MEMORY_EXTRACTION_PROMPT)
        )

        # Static product KB standing in for the vector search index
        self.kb_docs = [b.strip() for b in EXPERIAN_PRODUCTS.split("\n•") if b.strip()]

    async def aforward(self, query: str, context: RunContext) -> dspy.Prediction:
        return await self.run_pipeline(query, context)

    async def run_pipeline(self, query: str, context: RunContext) -> dspy.Prediction:
        # Step 1: Rewrite the query into self-contained sub-queries
        with dspy.context(lm=self.task_lm, send_stream=None):
            rewrite_result = await self.query_rewriter.acall(
                query=query, history=context.chat_history
            )
        rewritten = (rewrite_result.rewritten_queries or [query])[:MAX_DECOMPOSED_QUERIES]

        # Step 2: Retrieve context (plain I/O, deterministic)
        prioritized_docs, other_docs = self.retrieve_context(query, rewritten)
        formatted_prioritized = "\n\n".join(
            f"**Product #{i + 1}**\n• {doc}" for i, doc in enumerate(prioritized_docs)
        )
        formatted_others = "\n\n".join(
            f"**Product #{i + 1}**\n• {doc}" for i, doc in enumerate(other_docs)
        )

        # Step 3: Generate answer — long-term memory is a first-class input
        # NOTE: StreamListener listens only to the answer field of this call
        with dspy.context(lm=self.main_lm):
            pred = await self.answer_generator.acall(
                long_term_memory=context.long_term_memory,
                prioritized_context=formatted_prioritized,
                other_context=formatted_others,
                history=context.chat_history,
                query=query,
            )

        # Step 4: Generate memory content (LLM). Persisting it is plain I/O
        # done by the caller after the stream completes.
        pred.memories = []
        if self.config.enable_memory_extraction:
            try:
                with dspy.context(lm=self.memory_lm, send_stream=None):
                    extraction = await self.memory_extractor.acall(
                        history=context.chat_history, query=query, answer=pred.answer
                    )
                pred.memories = [m for m in (extraction.memories or []) if m and m.strip()]
            except Exception:
                logger.exception("memory extraction failed; persisting nothing this turn")
        return pred

    def retrieve_context(
        self, original_query: str, rewritten_queries: list[str]
    ) -> tuple[list[str], list[str]]:
        """Keyword-score the product catalog against all sub-queries."""
        terms: set[str] = set()
        for q in [original_query, *rewritten_queries]:
            terms.update(
                t for t in re.findall(r"[a-z0-9']+", q.lower()) if t not in _STOPWORDS
            )
        scored = sorted(
            (
                (sum(doc.lower().count(t) for t in terms), doc)
                for doc in self.kb_docs
            ),
            key=lambda pair: pair[0],
            reverse=True,
        )
        top = [doc for score, doc in scored if score > 0][: PRIORITIZED_TOP_K + OTHER_TOP_K]
        return top[:PRIORITIZED_TOP_K], top[PRIORITIZED_TOP_K:]
