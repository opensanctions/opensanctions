from functools import cache

from zavod.extract.names.clean import LLM_MODEL_VERSION, SINGLE_ENTITY_PROGRAM_PATH
from zavod.settings import OPENAI_API_KEY

import dspy  # type: ignore


class CleanNamesSignature(dspy.Signature):  # type: ignore
    """Names categorised and cleaned of non-name characters."""

    # Inputs
    entity_schema: str = dspy.InputField(
        desc="The schema denotes the type of entity. Both Persons and Organizations are specialisation of LegalEntity. Company is a specialisation of Organization. Everything extends Thing."
    )
    strings: list[str] = dspy.InputField(
        desc="A list of raw name strings to be cleaned and categorised. Each string might contain multiple names."
    )

    # Outputs
    name: list[str] = dspy.OutputField(
        desc="A list of the primary names of this entity, potentially in various languages and transliterations."
    )
    alias: list[str] = dspy.OutputField(
        desc="A list of alternative but still fully descriptive names for this entity."
    )
    weakAlias: list[str] = dspy.OutputField(
        desc="A list of names with low confidence or a very low degree of uniqueness in the context of legal entity names. Includes clear nicknames with no similarity to the full name."
    )
    previousName: list[str] = dspy.OutputField(
        desc="A list of names this entity was known by in the past."
    )
    abbreviation: list[str] = dspy.OutputField(
        desc="A list of acronyms or initialisms standing for the entity's name, e.g. 'TKB' for 'TRANSKAPITALBANK' or 'CCP NCC' for 'Central Counterparty National Clearing Centre'. Only when the input shows what the letters abbreviate. Not a variant that merely shortens the legal form, e.g. 'JSC Bank Ingo' for 'Joint Stock Company Bank Ingo' is an alias."
    )


@cache
def init_module() -> dspy.Predict:
    """Initialise a bare DSPy module for name splitting."""
    lm = dspy.LM(f"openai/{LLM_MODEL_VERSION}", api_key=OPENAI_API_KEY)
    dspy.configure(lm=lm)
    dspy.configure_cache(enable_disk_cache=False, enable_memory_cache=True)
    return dspy.Predict(CleanNamesSignature)


@cache
def load_optimised_module() -> dspy.Predict:
    """Load the optimised name splitting DSPy module."""
    module = init_module()
    module.load(SINGLE_ENTITY_PROGRAM_PATH)
    return module
