"""Build compact, model-facing outlines from rendered HTML.

The output preserves DOM order while removing empty elements and collapsing
single-child wrappers. Table cells (``td``, ``th``) keep their markers even
when empty or wrapping a single element: the cell markers carry the column
structure that pairs values across a row. Links and images retain resolved
resource URLs with fragments stripped, and images may include paths to bodies
stored in the snapshot's HAR. The body element is transparent: its text runs
and children render at the top level.

Example::

    - main:
      - h1 "Title"
      - a "Details" [href=https://example.com/details]
"""

import json
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote, urldefrag, urljoin

import lxml.html

from zavod.util import Element

# Elements whose content never reaches the rendered page.
SKIP_TAGS = frozenset({"head", "script", "style", "noscript", "template"})

# Cell markers are the column structure that pairs values across a row.
TABLE_CELL_TAGS = frozenset({"td", "th"})


def _require_http_base(url: str) -> None:
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"outline base URL must be absolute http(s), got {url!r}")


def build_outline(
    url: str, html: str, http_archive: dict[str, Any] | None = None
) -> str:
    """Render *html*, captured from *url*, as a compact model-facing outline.

    *url* is the page's final URL: the base for resolving ``src``/``href``
    and the exact key matched against HAR request URLs. *http_archive* is the
    snapshot's resolved HAR manifest, or ``None`` when the capture has none.
    An empty or whitespace-only *html* outlines to ``""``: a blank render is
    a capture the model gets to judge broken, not a parse error.
    """
    _require_http_base(url)
    if not html.strip():
        return ""
    document = lxml.html.document_fromstring(html)
    bodies = _har_bodies(http_archive)
    body = document.find("body")
    # Head-only documents parse without a body element; the document root
    # is then the closest thing to a body the capture has.
    root = body if body is not None else document
    return "\n".join(_element_lines(root, url, bodies))


def _element_lines(elem: Element, url: str, bodies: dict[str, str]) -> list[str]:
    """An empty return means nothing of the element is retained."""
    # Comments and other non-element nodes carry a non-string tag.
    if not isinstance(elem.tag, str) or elem.tag in SKIP_TAGS:
        return []
    # The body is transparent rather than rendered like an element.
    if elem.tag == "body":
        return _transparent_lines(elem, url, bodies)

    chunks = _chunks(elem, url, bodies)
    attrs = _kept_attrs(elem, url, bodies)
    # A single leading text run becomes the element's label; interleaved
    # runs stay in place as children so document order is preserved.
    label: str | None = None
    if (
        chunks
        and isinstance(chunks[0], str)
        and all(not isinstance(chunk, str) for chunk in chunks[1:])
    ):
        label = chunks[0]
        chunks = chunks[1:]
    is_cell = elem.tag in TABLE_CELL_TAGS
    if not is_cell and label is None and not attrs and not chunks:
        return []
    # A wrapper adding nothing of its own folds into its single child, so
    # container chains collapse bottom-up into the element that matters.
    if (
        not is_cell
        and label is None
        and not attrs
        and len(chunks) == 1
        and isinstance(chunks[0], list)
    ):
        return chunks[0]
    key = elem.tag if label is None else f"{elem.tag} {_quote(label)}"
    key += "".join(f" [{name}={value}]" for name, value in attrs)
    if not chunks:
        return [f"- {key}"]
    children: list[str] = []
    for chunk in chunks:
        if isinstance(chunk, str):
            children.append(f"- text: {_quote(chunk)}")
        else:
            children.extend(chunk)
    return [f"- {key}:"] + [f"  {line}" for line in children]


def _transparent_lines(elem: Element, url: str, bodies: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for chunk in _chunks(elem, url, bodies):
        if isinstance(chunk, str):
            lines.append(f"- text: {_quote(chunk)}")
        else:
            lines.extend(chunk)
    return lines


def _chunks(elem: Element, url: str, bodies: dict[str, str]) -> list[str | list[str]]:
    """The element's content in document order: a text run as ``str``, a
    retained child element as its rendered lines."""
    chunks: list[str | list[str]] = []
    if elem.text is not None and (text := _normalize(elem.text)):
        chunks.append(text)
    for child in elem:
        if lines := _element_lines(child, url, bodies):
            chunks.append(lines)
        # A dropped element's trailing text survives it as a text run.
        if child.tail is not None and (tail := _normalize(child.tail)):
            chunks.append(tail)
    return _merge_adjacent_text(chunks)


def _kept_attrs(
    elem: Element, url: str, bodies: dict[str, str]
) -> list[tuple[str, str]]:
    attrs: list[tuple[str, str]] = []
    if elem.tag == "a":
        href = elem.get("href")
        if href is not None:
            target, _fragment = urldefrag(urljoin(url, href))
            attrs.append(("href", target))
    elif elem.tag == "img":
        src = elem.get("src")
        if src is not None:
            target, _fragment = urldefrag(urljoin(url, src))
            resolved = quote(target, safe="/:?#[]@!$&'()*+,;=%")
            attrs.append(("src", resolved))
            if resolved in bodies:
                attrs.append(("body", bodies[resolved]))
        alt = elem.get("alt")
        if alt is not None:
            attrs.append(("alt", alt))
    return attrs


def har_resource_media_types(http_archive: dict[str, Any] | None) -> dict[str, str]:
    """Map each stored body path to its normalized response media type.

    Untyped bodies are omitted rather than guessed at: ad and telemetry
    beacons routinely respond without a Content-Type.
    """
    media: dict[str, str] = {}
    for _url, file, content in _stored_bodies(http_archive):
        media_type = _normalized_media_type(content.get("mimeType"))
        if media_type is not None:
            media[file] = media_type
    return media


def _stored_bodies(
    http_archive: dict[str, Any] | None,
) -> Iterator[tuple[str, str, dict[str, Any]]]:
    if http_archive is None:
        return
    for entry in http_archive["log"]["entries"]:
        content = entry["response"]["content"]
        file = content.get("_file")
        if file:
            yield entry["request"]["url"], file, content


def _normalized_media_type(mime_type: str | None) -> str | None:
    """Normalize a HAR mimeType; ``None`` when no usable type exists.

    A Playwright mimeType is the origin's Content-Type header verbatim,
    with ``x-unknown`` its placeholder for a response that carried none;
    anything can appear, so consumers judge what is usable.
    """
    if mime_type is None:
        return None
    media_type = mime_type.partition(";")[0].strip().lower()
    return None if media_type in ("", "x-unknown") else media_type


def _har_bodies(http_archive: dict[str, Any] | None) -> dict[str, str]:
    """The latest entry with a stored body wins; bodyless entries never
    shadow an earlier body for the same URL."""
    bodies: dict[str, str] = {}
    for url, file, _content in _stored_bodies(http_archive):
        bodies[url] = file
    return bodies


def _merge_adjacent_text(chunks: list[str | list[str]]) -> list[str | list[str]]:
    """Join neighbouring text runs dropped-element gaps leave behind."""
    merged: list[str | list[str]] = []
    for chunk in chunks:
        if isinstance(chunk, str) and merged and isinstance(merged[-1], str):
            merged[-1] = f"{merged[-1]} {chunk}"
        else:
            merged.append(chunk)
    return merged


def _normalize(text: str) -> str:
    return " ".join(text.split())


def _quote(text: str) -> str:
    """Quote a text value as a JSON string, keeping non-ASCII readable."""
    return json.dumps(text, ensure_ascii=False)
