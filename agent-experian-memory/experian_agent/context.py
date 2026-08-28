from pydantic import BaseModel


class RunContext(BaseModel):
    """Runtime context for one pipeline run.

    chat_history is the transcript rebuilt from the session store (the client
    no longer needs to resend prior turns); long_term_memory is the formatted
    result of the pre-turn memory-store search.
    """

    chat_history: str = ""
    long_term_memory: str = ""
