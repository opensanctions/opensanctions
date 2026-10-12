"""Tests for the page metadata slice and prompt assembly."""

import pytest

from zavod.shed.funes.page import PageMetadata, build_prompt, metadata_from_html


def test_build_prompt_contains_only_delimited_page_context() -> None:
    metadata = PageMetadata(
        requested_url="https://example.org/x",
        final_url="https://example.org/y",
        title="T",
        description=None,
    )
    prompt = build_prompt(metadata, '- h1 "Cabinet"')
    assert prompt.startswith("<page_snapshot>")
    assert "<objective>" not in prompt
    assert "<page_metadata>" in prompt
    assert "<page_outline>" in prompt


def test_build_prompt_marks_context_and_includes_error() -> None:
    metadata = PageMetadata(
        requested_url="https://example.org/x",
        final_url="https://example.org/y",
        http_status=503,
        capture_error="navigation timeout",
        title="T",
        description=None,
    )
    outline = (
        '- main:\n  - h1 "Board"\n  - img "logo" '
        "[src=https://example.org/l.png] [body=bodies/l.png]"
    )
    prompt = build_prompt(metadata, outline)
    assert "<page_snapshot>" in prompt and "</page_snapshot>" in prompt
    assert "Requested URL: https://example.org/x" in prompt
    assert "Final URL: https://example.org/y" in prompt
    assert "HTTP status: 503" in prompt
    assert "Capture error: navigation timeout" in prompt
    assert "Document title: T" in prompt
    assert "Meta description: [not provided]" in prompt
    assert f"<page_outline>\n{outline}\n</page_outline>" in prompt


def test_build_prompt_omits_absent_optional_fields() -> None:
    metadata = PageMetadata(
        requested_url="https://example.org",
        final_url="https://example.org",
        title=None,
        description=None,
    )
    prompt = build_prompt(metadata, '- text: "empty"')
    assert "Capture error" not in prompt
    assert "HTTP status: [not provided]" in prompt
    assert '<page_outline>\n- text: "empty"\n</page_outline>' in prompt


def test_metadata_from_html_fields() -> None:
    html = """
    <html><head><title>Board — Example</title>
    <meta name="description" content="The board.">
    </head><body></body></html>
    """
    metadata = metadata_from_html(
        "https://example.org/board",
        html,
        final_url="https://www.example.org/board/",
        http_status=200,
        capture_error=None,
    )
    assert metadata.requested_url == "https://example.org/board"
    assert metadata.final_url == "https://www.example.org/board/"
    assert metadata.http_status == 200
    assert metadata.capture_error is None
    assert metadata.title == "Board — Example"
    assert metadata.description == "The board."


def test_metadata_from_html_requires_final_url() -> None:
    with pytest.raises(TypeError):
        metadata_from_html("https://example.org", "<html></html>")  # type: ignore[call-arg]


def test_metadata_from_html_empty_page() -> None:
    metadata = metadata_from_html(
        "https://example.org", "<html></html>", final_url="https://example.org"
    )
    assert metadata.title is None
    assert metadata.description is None
    assert metadata.http_status is None
    assert metadata.capture_error is None


def test_metadata_from_html_empty_capture() -> None:
    """A blank render is model data, not a parse error."""
    for html in ("", "  \n "):
        metadata = metadata_from_html(
            "https://example.org", html, final_url="https://example.org"
        )
        assert metadata.title is None
        assert metadata.description is None


def test_metadata_from_html_open_graph_fallbacks() -> None:
    html = """
    <html><head>
    <meta property="og:title" content="OG Title">
    <meta property="og:description" content="OG description.">
    </head><body></body></html>
    """
    metadata = metadata_from_html(
        "https://example.org", html, final_url="https://example.org"
    )
    assert metadata.title == "OG Title"
    assert metadata.description == "OG description."


def test_metadata_from_html_empty_description_beats_open_graph() -> None:
    """A present-but-empty description meta wins over the og fallback."""
    html = """
    <html><head><meta name="description">
    <meta property="og:description" content="OG description.">
    </head><body></body></html>
    """
    metadata = metadata_from_html(
        "https://example.org", html, final_url="https://example.org"
    )
    assert metadata.description is None
