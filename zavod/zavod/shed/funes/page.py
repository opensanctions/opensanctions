"""The captured page as model context: a metadata slice and prompt assembly."""

import lxml.html
from pydantic import BaseModel, ConfigDict

from zavod import helpers as h
from zavod.util import Element


class PageMetadata(BaseModel):
    """Small, explicit slice of page metadata supplied to the model."""

    model_config = ConfigDict(extra="forbid")

    requested_url: str
    final_url: str
    http_status: int | None = None
    capture_error: str | None = None
    title: str | None
    description: str | None


def build_prompt(metadata: PageMetadata, outline: str) -> str:
    """Build one user message from the captured page snapshot.

    The prompt carries untrusted page content only; the trusted inspection
    brief travels separately as run instructions. Fields with no value are
    omitted entirely: the absence of a line is the signal, so no placeholder
    text is ever supplied.
    """
    diagnostics = [
        "Requested URL: " + metadata.requested_url,
        "Final URL: " + metadata.final_url,
    ]
    if metadata.http_status is not None:
        diagnostics.append(f"HTTP status: {metadata.http_status}")
    if metadata.capture_error is not None:
        diagnostics.append(f"Capture error: {metadata.capture_error}")
    source_metadata = []
    if metadata.title is not None:
        source_metadata.append(f"Document title: {metadata.title}")
    if metadata.description is not None:
        source_metadata.append(f"Meta description: {metadata.description}")
    sections = [
        "<page_diagnostics>\n" + "\n".join(diagnostics) + "\n</page_diagnostics>",
    ]
    if source_metadata:
        sections.append(
            "<page_metadata>\n" + "\n".join(source_metadata) + "\n</page_metadata>"
        )
    sections.append(f"<page_outline>\n{outline}\n</page_outline>")
    return "<page_snapshot>\n" + "\n\n".join(sections) + "\n</page_snapshot>"


def _meta_content(document: Element, xpath: str) -> str | None:
    """A matching element without a content attribute counts as present
    with empty content: like funes, it wins over the Open Graph fallback."""
    elements = h.xpath_elements(document, xpath)
    if not elements:
        return None
    return elements[0].get("content") or ""


def metadata_from_html(
    requested_url: str,
    html: str,
    *,
    final_url: str,
    http_status: int | None = None,
    capture_error: str | None = None,
) -> PageMetadata:
    """Extract the small metadata slice used as model context.

    Open Graph values are fallbacks only: the document title and standard meta
    description win when both forms are present. The URLs, HTTP status, and
    capture error are included as context but the model is explicitly forbidden
    from treating them as evidence. An empty or whitespace-only *html* yields
    empty metadata: a blank render is a capture the model gets to judge
    broken, not a parse error.
    """
    title = ""
    description: str | None = None
    if html.strip():
        document = lxml.html.document_fromstring(html)
        title = h.element_text(document.find(".//title"))
        if not title:
            og_title = _meta_content(document, "//meta[@property='og:title']")
            title = og_title.strip() if og_title is not None else ""
        description = _meta_content(document, "//meta[@name='description']")
        if description is None:
            description = _meta_content(document, "//meta[@property='og:description']")
        if description is not None:
            description = description.strip()
    return PageMetadata(
        requested_url=requested_url,
        final_url=final_url,
        http_status=http_status,
        capture_error=capture_error,
        title=title or None,
        description=description or None,
    )
