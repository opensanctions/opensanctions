import json
import base64
import logging
import mimetypes
from hashlib import sha1
from pathlib import Path
from openai import OpenAI, AzureOpenAI
from typing import Optional, Any, Type, TypeVar
from functools import cache

from pydantic import BaseModel

from zavod import settings
from zavod.logs import get_logger
from zavod.context import Context
from zavod.exc import ConfigurationException

log = get_logger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

T = TypeVar("T", bound=BaseModel)


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
    response_type: Optional[Type[T]] = None,
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
        response_format=response_type or {"type": "json_object"},
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
    response_type: Type[T],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> T:
    data = run_image_prompt(
        context,
        prompt,
        image_path,
        max_tokens,
        cache_days,
        model,
        response_type,
    )
    return response_type.model_validate(data)


def run_text_prompt(
    context: Context,
    prompt: str,
    string: str,
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
    response_type: Optional[Type[T]] = None,
) -> Any:
    """Run a text prompt."""
    client = get_client()
    cache_hash = sha1(string.encode("utf-8"))
    cache_hash.update(prompt.encode("utf-8"))
    if response_type is not None:
        cache_hash.update(json.dumps(response_type.model_json_schema()).encode("utf-8"))
    cache_key = cache_hash.hexdigest()
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        log.info("GPT cache hit: %s" % string[:50])
        return cached_data
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
        response_format=response_type or {"type": "json_object"},
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
    response_type: Type[T],
    max_tokens: int = 3000,
    cache_days: int = 100,
    model: str = "gpt-4o",
) -> T:
    data = run_text_prompt(
        context,
        prompt,
        string,
        max_tokens,
        cache_days,
        model,
        response_type,
    )
    return response_type.model_validate(data)
