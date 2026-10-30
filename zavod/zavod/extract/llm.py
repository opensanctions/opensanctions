import json
import logging
from dataclasses import dataclass
from functools import cache
from hashlib import sha1
from pathlib import Path
from typing import Any, TypeVar

import pydantic_ai
from pydantic import BaseModel
from pydantic_ai import Agent, BinaryContent, UserContent
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model, infer_model
from pydantic_ai.settings import ModelSettings

from zavod.context import Context
from zavod.exc import ConfigurationException
from zavod.logs import get_logger

DEFAULT_MODEL = "openai:gpt-4o"

log = get_logger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
pydantic_ai.BANNER_ENABLED = False

ResponseType = TypeVar("ResponseType", bound=BaseModel)


@cache
def get_model(name: str) -> Model:
    """Get a model by name.

    Names are given as `provider:model`, e.g. `openai:gpt-4o`,
    `openrouter:anthropic/claude-sonnet-4.6` or `ollama:qwen3`. Providers read
    their credentials from their standard environment variables, e.g.
    `OPENAI_API_KEY` or `OPENROUTER_API_KEY`.

    Models are cached because each holds its own HTTP connection pool."""
    try:
        return infer_model(name)
    except UserError as exc:
        # Missing credentials or an unknown provider.
        raise ConfigurationException(str(exc)) from exc


def run_prompt[OutputType](
    model: str,
    user_prompt: list[UserContent],
    output_type: type[OutputType],
    max_tokens: int,
) -> OutputType:
    """Run a single-turn prompt and return the validated output."""
    agent = Agent(
        get_model(model),
        output_type=output_type,
        model_settings=ModelSettings(max_tokens=max_tokens),
    )
    return agent.run_sync(user_prompt).output


def run_image_prompt(
    context: Context,
    prompt: str,
    image_path: Path,
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = DEFAULT_MODEL,
) -> Any:
    """Run an image prompt."""
    image = BinaryContent.from_path(image_path)
    cache_hash = sha1(image.data_uri.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info(f"LLM cache hit: {image_path.name}")
        return cached_data
    log.info(f"Prompting {model!r} for: {image_path.name}")
    data = run_prompt(model, [prompt, image], dict[str, Any], max_tokens)
    context.cache.set_json(cache_key, data)
    return data


def run_typed_image_prompt[ResponseType: BaseModel](
    context: Context,
    prompt: str,
    image_path: Path,
    response_type: type[ResponseType],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = DEFAULT_MODEL,
) -> ResponseType:
    """Run an image prompt."""
    image = BinaryContent.from_path(image_path)
    cache_hash = sha1(image.data_uri.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    json_schema = response_type.model_json_schema()
    cache_hash.update(json.dumps(json_schema, sort_keys=True).encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info(f"LLM cache hit: {image_path.name}")
        return response_type.model_validate(cached_data)
    log.info(f"Prompting {model!r} for: {image_path.name}")
    output = run_prompt(model, [prompt, image], response_type, max_tokens)
    context.cache.set_json(cache_key, output.model_dump(mode="json"))
    return output


@dataclass
class TextPromptResponse:
    content: str
    cache_key: str
    """The cache key for the response.

    This is returned regardless of whether the response was cached or not.
    It can be used by callers to invalidate the cache when the response turns out to
    be dumb and needs to be retried on a day with better weather.
    """


def run_text_prompt(
    context: Context,
    prompt: str,
    string: str,
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = DEFAULT_MODEL,
) -> TextPromptResponse:
    """Run a text prompt. The response content is a JSON object."""
    cache_hash = sha1(string.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info(f"LLM cache hit: {string[:50]}")
        return TextPromptResponse(content=cached_data, cache_key=cache_key)
    log.info(f"Prompting {model!r} for: {string[:50]}")
    data = run_prompt(model, [prompt, string], dict[str, Any], max_tokens)
    content = json.dumps(data)
    context.cache.set(cache_key, content)
    return TextPromptResponse(content=content, cache_key=cache_key)


def run_typed_text_prompt[ResponseType: BaseModel](
    context: Context,
    prompt: str,
    string: str,
    response_type: type[ResponseType],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = DEFAULT_MODEL,
) -> ResponseType:
    """Run a text prompt."""
    cache_hash = sha1(string.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    json_schema = response_type.model_json_schema()
    cache_hash.update(json.dumps(json_schema, sort_keys=True).encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info(f"LLM cache hit: {string[:50]}")
        return response_type.model_validate(cached_data)
    log.info(f"Prompting {model!r} for: {string[:50]}")
    output = run_prompt(model, [prompt, string], response_type, max_tokens)
    context.cache.set_json(cache_key, output.model_dump(mode="json"))
    return output
