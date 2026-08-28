import dspy


class QueryRewriterSignature(dspy.Signature):
    history: str = dspy.InputField(
        description="Prior conversation transcript.",
        default="",
    )
    query: str = dspy.InputField()
    # ========
    rewritten_queries: list[str] = dspy.OutputField(
        description="Self-contained search queries capturing the user's intent."
    )


class AnswerGeneratorSignature(dspy.Signature):
    long_term_memory: str = dspy.InputField(
        description=(
            "Durable facts remembered about this user from previous conversations. "
            "May be empty for new users."
        ),
        default="",
    )
    prioritized_context: str = dspy.InputField(
        description="The prioritized context documents relevant to the user query."
    )
    other_context: str = dspy.InputField(
        description="Only use other context documents if prioritized context is insufficient."
    )
    history: str = dspy.InputField(
        description="Prior conversation transcript.",
        default="",
    )
    query: str = dspy.InputField()
    # ========
    answer: str = dspy.OutputField()


class MemoryExtractorSignature(dspy.Signature):
    history: str = dspy.InputField(
        description="Prior conversation transcript.",
        default="",
    )
    query: str = dspy.InputField(description="The user's message this turn.")
    answer: str = dspy.InputField(description="The assistant's answer this turn.")
    # ========
    memories: list[str] = dspy.OutputField(
        description="Durable user facts to persist; empty if none."
    )
