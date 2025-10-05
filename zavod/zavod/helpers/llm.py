from zavod.context import Context
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import LLMExtractionConfig, SourceValue, ModelType


def prompt_for_review(
    context: Context,
    extraction_config: LLMExtractionConfig[ModelType],
    source_value: SourceValue,
) -> ModelType:
    return run_typed_text_prompt(
        context,
        extraction_config.prompt,
        source_value.value_string,
        extraction_config.data_model,
        model=extraction_config.llm_model,
    )
