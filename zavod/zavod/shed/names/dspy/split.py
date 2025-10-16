from functools import cache
from typing import List

from zavod.settings import OPENAI_API_KEY
from zavod.shed.names.split import LLM_MODEL_VERSION, SINGLE_ENTITY_PROGRAM_PATH

import dspy  # type: ignore


class SplitNamesSignature(dspy.Signature):  # type: ignore
    """Names categorised and cleaned of non-name characters."""

    string: str = dspy.InputField()
    full_name: List[str] = dspy.OutputField(
        desc="A list of the names of this entity, potentially in various languages and transliterations."
    )
    alias: list[str] = dspy.OutputField(
        desc="A list of alternative names or nicknames for this entity."
    )
    weak_alias: list[str] = dspy.OutputField(
        desc="A list of names with low confidence or a very low degree of uniqueness in the context of legal entity names."
    )
    previous_name: list[str] = dspy.OutputField(
        desc="A list of names this entity was known by in the past."
    )


@cache
def init_module() -> dspy.Predict:
    """Initialise a bare DSPy module for name splitting."""
    lm = dspy.LM(f"openai/{LLM_MODEL_VERSION}", api_key=OPENAI_API_KEY)
    dspy.configure(lm=lm)
    dspy.configure_cache(enable_disk_cache=False, enable_memory_cache=True)
    return dspy.Predict(SplitNamesSignature)


@cache
def load_optimised_module() -> dspy.Predict:
    """Load the optimised name splitting DSPy module."""
    module = init_module()
    module.load(SINGLE_ENTITY_PROGRAM_PATH)
    return module
