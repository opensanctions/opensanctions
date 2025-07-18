import base64
import json
import logging
import mimetypes
from functools import cache
from hashlib import sha1
from pathlib import Path
from typing import Any, Optional, Type, TypeVar

from openai import AzureOpenAI, OpenAI
from pydantic import BaseModel

from zavod import settings
from zavod.context import Context
from zavod.exc import ConfigurationException
from zavod.logs import get_logger

log = get_logger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

ResponseType = TypeVar("ResponseType", bound=BaseModel)


@cache
def get_client() -> OpenAI:
    """Get the OpenAI client."""
    if settings.OPENAI_API_KEY is None:
        raise ConfigurationException("No $OPENSANCTIONS_OPENAI_API_KEY key provided.")
    if settings.AZURE_OPENAI_ENDPOINT is not None:
        return AzureOpenAI(
            api_key=settings.OPENAI_API_KEY,
            api_version="2023-12-01-preview",
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
        )
    return OpenAI(api_key=settings.OPENAI_API_KEY)


def encode_file(file: Path, mime_type: Optional[str] = None) -> str:
    """Encode a file as a base64 data URL."""
    if mime_type is None:
        mime_type, _ = mimetypes.guess_type(file.name)
    with open(file, "rb") as f:
        data = f.read()
        encoded = base64.b64encode(data).decode("utf-8")
        return f"data:{mime_type};base64,{encoded}"


def run_image_prompt(
    context: Context,
    prompt: str,
    image_path: Path,
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> Any:
    """Run an image prompt."""
    client = get_client()
    image_url = encode_file(image_path)
    cache_hash = sha1(image_url.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info("GPT cache hit: %s" % image_path.name)
        return cached_data
    log.info("Prompting %r for: %s" % (model, image_path.name))
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
        response_format={"type": "json_object"},
        max_tokens=max_tokens,
    )
    assert len(response.choices) > 0
    assert response.choices[0].message is not None
    assert response.choices[0].message.content is not None
    data = json.loads(response.choices[0].message.content)
    context.cache.set_json(cache_key, data)
    return data


def run_typed_image_prompt(
    context: Context,
    prompt: str,
    image_path: Path,
    response_type: Type[ResponseType],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> ResponseType:
    """Run an image prompt."""
    client = get_client()
    image_url = encode_file(image_path)
    cache_hash = sha1(image_url.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    json_schema = response_type.model_json_schema()
    cache_hash.update(json.dumps(json_schema, sort_keys=True).encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info("GPT cache hit: %s" % image_path.name)
        return response_type.model_validate(cached_data)
    log.info("Prompting %r for: %s" % (model, image_path.name))
    response = client.chat.completions.parse(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
        response_format=response_type,
        max_tokens=max_tokens,
    )
    assert len(response.choices) > 0
    assert response.choices[0].message is not None
    assert response.choices[0].message.parsed is not None
    data = response.choices[0].message.parsed
    context.cache.set_json(cache_key, data)
    return data


def run_text_prompt(
    context: Context,
    prompt: str,
    string: str,
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> Any:
    """Run a text prompt."""
    client = get_client()
    cache_hash = sha1(string.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info("GPT cache hit: %s" % string[:50])
        return cached_data
    log.info("Prompting %r for: %s" % (model, string[:50]))
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "text", "text": string},
                ],
            }
        ],
        response_format={"type": "json_object"},
        max_tokens=max_tokens,
    )
    assert len(response.choices) > 0
    assert response.choices[0].message is not None
    assert response.choices[0].message.content is not None
    data = json.loads(response.choices[0].message.content)
    context.cache.set_json(cache_key, data)
    return data


def run_typed_text_prompt(
    context: Context,
    prompt: str,
    string: str,
    response_type: Type[ResponseType],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> ResponseType:
    """Run a text prompt."""
    client = get_client()
    cache_hash = sha1(string.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    json_schema = response_type.model_json_schema()
    cache_hash.update(json.dumps(json_schema, sort_keys=True).encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info("GPT cache hit: %s" % string[:50])
        return response_type.model_validate(cached_data)
    log.info("Prompting %r for: %s" % (model, string[:50]))
    response = client.chat.completions.parse(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "text", "text": string},
                ],
            }
        ],
        response_format=response_type,
        max_tokens=max_tokens,
    )
    assert len(response.choices) > 0
    assert response.choices[0].message is not None
    assert response.choices[0].message.content is not None
    json_data = json.loads(response.choices[0].message.content)
    context.cache.set_json(cache_key, json_data)
    assert response.choices[0].message.parsed is not None
    structured_data = response.choices[0].message.parsed
    return structured_data
