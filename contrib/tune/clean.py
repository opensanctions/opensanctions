from functools import cache
import json
import os
from pathlib import Path
from typing import Any

import dspy  # type: ignore

# The model the prompt is tuned and deployed with. This is the only place it
# is chosen: it is written into the program artifact on save, and zavod reads
# it back from there in production.
MODEL = "gpt-5.4"

# The program artifact is the deliverable of this tooling and the only file
# shared with zavod, which loads and validates it on start-up.
PROGRAM_PATH = (
    Path(__file__)
    .resolve()
    .parents[2]
    .joinpath("zavod", "zavod", "extract", "names", "single_entity_program.json")
)


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


def format_input_string(entity_schema: str, strings: list[str]) -> str:
    """Format the LLM input exactly as production does.

    Keep in sync with the ``input_string`` construction in
    ``zavod.extract.names.clean.clean_names``.
    """
    input_data = {"entity_schema": entity_schema, "strings": strings}
    return "The entity schema and name strings as JSON:\n\n" + json.dumps(
        input_data, indent=2, ensure_ascii=False
    )


class ProductionFormatAdapter(dspy.JSONAdapter):  # type: ignore
    """Run LM calls with exactly the wire format production uses.

    Production (``zavod.extract.llm.run_typed_text_prompt``) sends one user
    message with the prompt and the input JSON as two text parts, and
    constrains the response with a JSON schema. This adapter formats calls
    the same way, so GEPA optimises the prompt and ``compare`` evaluates it
    under deployment conditions rather than inside dspy's default chat
    scaffolding, which production never sends. The JSON-schema-constrained
    output is inherited from JSONAdapter, which requests an OpenAI-style
    ``response_format``.

    Few-shot demos have no production equivalent, so their presence is an
    error rather than a silent divergence.
    """

    def format(
        self, signature: Any, demos: list[Any], inputs: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert not demos, "demos cannot be rendered in the production format"
        return [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": signature.instructions},
                    {
                        "type": "text",
                        "text": format_input_string(
                            inputs["entity_schema"], inputs["strings"]
                        ),
                    },
                ],
            }
        ]


def get_openai_api_key() -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key is None:
        raise RuntimeError("Set $OPENAI_API_KEY to run the tuning tools.")
    return api_key


@cache
def init_module() -> dspy.Predict:
    """Initialise a bare DSPy module for name splitting."""
    lm = dspy.LM(f"openai/{MODEL}", api_key=get_openai_api_key())
    dspy.configure(lm=lm)
    dspy.configure(adapter=ProductionFormatAdapter())
    # Cache LM responses on disk so repeated compare runs don't re-bill.
    # Cache keys include the prompt, so optimisation is unaffected.
    dspy.configure_cache(enable_disk_cache=True, enable_memory_cache=True)
    return dspy.Predict(CleanNamesSignature)


@cache
def load_optimised_module() -> dspy.Predict:
    """Load the optimised name splitting DSPy module."""
    program_data = json.loads(PROGRAM_PATH.read_text())
    artifact_model = program_data.get("model")
    assert artifact_model == MODEL, (
        f"Program artifact {PROGRAM_PATH} was tuned for {artifact_model!r}, "
        f"but this tooling runs {MODEL!r}."
    )
    module = init_module()
    module.load(PROGRAM_PATH)
    return module
