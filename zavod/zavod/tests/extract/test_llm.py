import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from pydantic import BaseModel

from zavod.context import Context
from zavod.extract.llm import run_typed_image_prompt, run_typed_text_prompt


class Extracted(BaseModel):
    name: str


def mock_client() -> MagicMock:
    client = MagicMock()
    message = MagicMock()
    message.content = json.dumps({"name": "John Doe"})
    message.parsed = Extracted(name="John Doe")
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    client.chat.completions.parse.return_value = response
    return client


@patch("zavod.extract.llm.get_client")
def test_typed_text_prompt_cache_key_includes_model(
    get_client: MagicMock, vcontext: Context
) -> None:
    client = mock_client()
    get_client.return_value = client
    parse = client.chat.completions.parse

    result = run_typed_text_prompt(
        vcontext, "prompt", "input string", Extracted, model="model-a"
    )
    assert result.name == "John Doe"
    assert parse.call_count == 1

    # Same input, prompt, schema and model: served from the cache.
    cached = run_typed_text_prompt(
        vcontext, "prompt", "input string", Extracted, model="model-a"
    )
    assert cached.name == "John Doe"
    assert parse.call_count == 1

    # A different model must not be served the other model's cached response.
    run_typed_text_prompt(
        vcontext, "prompt", "input string", Extracted, model="model-b"
    )
    assert parse.call_count == 2


@patch("zavod.extract.llm.get_client")
def test_typed_image_prompt_cache_key_includes_model(
    get_client: MagicMock, vcontext: Context, tmp_path: Path
) -> None:
    client = mock_client()
    get_client.return_value = client
    parse = client.chat.completions.parse
    image_path = tmp_path / "image.png"
    image_path.write_bytes(b"\x89PNG not really an image")

    result = run_typed_image_prompt(
        vcontext, "prompt", image_path, Extracted, model="model-a"
    )
    assert result.name == "John Doe"
    assert parse.call_count == 1

    # Same image, prompt, schema and model: served from the cache.
    cached = run_typed_image_prompt(
        vcontext, "prompt", image_path, Extracted, model="model-a"
    )
    assert cached.name == "John Doe"
    assert parse.call_count == 1

    # A different model must not be served the other model's cached response.
    run_typed_image_prompt(vcontext, "prompt", image_path, Extracted, model="model-b")
    assert parse.call_count == 2
