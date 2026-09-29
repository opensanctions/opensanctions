"""Render one dataset's fixtures as a static HTML page for quick human review:
each case shows the source document beside the accepted extraction, what the
reviewer changed, and where the case lives in the fixtures file."""

import html
import re
from pathlib import Path
from typing import Any

import yaml
from lxml.html import HtmlElement, fromstring, tostring
from pydantic_evals import Case

from contrib.prompt_evals.coverage import coverage_counts
from contrib.prompt_evals.models import CaseInputs, CaseMeta, Extraction, FixtureDataset

STYLE = """
body { font-family: system-ui, sans-serif; margin: 0; color: #222; }
header, section { padding: 1rem 2rem; }
header { background: #f4f4f4; border-bottom: 1px solid #ddd; }
section { border-bottom: 1px solid #ddd; }
h2 { margin: 0 0 .25rem 0; }
.meta { color: #555; font-size: .9rem; margin-bottom: .75rem; }
.meta a, .meta code { margin-right: 1rem; }
.tag { display: inline-block; background: #e8eef7; border-radius: 3px; padding: 0 .4rem; margin: 0 .2rem .2rem 0; font-size: .8rem; }
.tag.rule { background: #f7e8c8; }
.columns { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
iframe { width: 100%; height: 640px; border: 1px solid #ccc; background: white; }
pre { background: #fafafa; border: 1px solid #ccc; padding: .75rem; overflow: auto; max-height: 640px; margin: 0; font-size: .85rem; }
.edits li { font-family: monospace; font-size: .85rem; }
table.coverage { border-collapse: collapse; font-size: .85rem; }
table.coverage td { padding: 0 .75rem 0 0; }
nav ol { columns: 3; font-size: .9rem; }
"""


SKIP_HIGHLIGHT_FIELDS = {"entity_schema"}
"""Fields whose values are labels rather than source text, e.g. 'Person' would
otherwise highlight the word wherever the article uses it."""


def extracted_strings(extraction: Extraction) -> list[str]:
    """Every string value in the extraction, longest first so that a longer
    value is highlighted in preference to a shorter one it contains."""
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, str):
            if value.strip():
                found.add(value.strip())
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                if key not in SKIP_HIGHLIGHT_FIELDS:
                    walk(item)

    walk(extraction)
    return sorted(found, key=len, reverse=True)


def _mark_text(
    text: str | None, pattern: re.Pattern[str]
) -> tuple[str | None, list[HtmlElement]]:
    """Split `text` at matches, returning the leading unmatched text and a list of
    <mark> elements whose tails carry the unmatched text that follows each."""
    if not text or not pattern.search(text):
        return text, []
    parts = pattern.split(text)
    marks: list[HtmlElement] = []
    for index in range(1, len(parts), 2):
        mark: HtmlElement = fromstring("<mark></mark>")
        mark.text = parts[index]
        mark.tail = parts[index + 1] or None
        marks.append(mark)
    return parts[0] or None, marks


def highlight_html(source: str, terms: list[str]) -> str:
    """Wrap case-insensitive occurrences of `terms` in the text of `source` with
    <mark>, and mark links whose target is one of the terms."""
    if not terms:
        return source
    pattern = re.compile(
        "(" + "|".join(re.escape(t) for t in terms) + ")", re.IGNORECASE
    )
    hrefs = {t.casefold() for t in terms}
    root: HtmlElement = fromstring(source)
    for element in list(root.iter()):
        if not isinstance(element.tag, str) or element.tag in ("script", "style"):
            continue
        if element.tag == "a" and (element.get("href") or "").casefold() in hrefs:
            element.set("style", "background: #ff0")
        element.text, marks = _mark_text(element.text, pattern)
        for offset, mark in enumerate(marks):
            element.insert(offset, mark)
        for child in list(element):
            child.tail, marks = _mark_text(child.tail, pattern)
            position = element.index(child)
            for offset, mark in enumerate(marks, start=1):
                element.insert(position + offset, mark)
    return str(tostring(root, encoding="unicode"))


def highlight_text(source: str, terms: list[str]) -> str:
    """HTML-escaped plain text with case-insensitive occurrences of `terms` marked."""
    if not terms:
        return html.escape(source)
    pattern = re.compile(
        "(" + "|".join(re.escape(t) for t in terms) + ")", re.IGNORECASE
    )
    parts = pattern.split(source)
    return "".join(
        f"<mark>{html.escape(part)}</mark>" if index % 2 else html.escape(part)
        for index, part in enumerate(parts)
    )


def case_line_numbers(fixtures_file: Path) -> dict[str, int]:
    """Line of each `- name:` entry in the fixtures file, for jumping to it in an editor."""
    lines: dict[str, int] = {}
    for number, line in enumerate(fixtures_file.read_text().splitlines(), start=1):
        match = re.match(r"^- name: (.+)$", line)
        if match:
            lines[match.group(1).strip().strip("'\"")] = number
    return lines


def render_case(
    case: Case[CaseInputs, Extraction, CaseMeta],
    fixtures_file: Path,
    line: int | None,
    tags: set[str],
    ide_link: str | None,
) -> str:
    assert case.expected_output is not None and case.metadata is not None
    name = str(case.name)
    source = (fixtures_file.parent / case.inputs.source_file).read_text()
    items = case.expected_output.get("designees") or []

    links = []
    if case.inputs.url:
        links.append(
            f'<a href="{html.escape(case.inputs.url)}" target="_blank">source page</a>'
        )
    location = f"{fixtures_file}:{line}" if line else str(fixtures_file)
    if ide_link and line:
        href = ide_link.format(path=fixtures_file.resolve(), line=line)
        links.append(f'<a href="{html.escape(href)}">{html.escape(location)}</a>')
    else:
        links.append(f"<code>{html.escape(location)}</code>")

    rule_tags = "".join(
        f'<span class="tag rule">{html.escape(r)}</span>' for r in case.metadata.rules
    )
    derived = "".join(
        f'<span class="tag">{html.escape(t)}</span>'
        for t in sorted(tags - set(case.metadata.rules))
    )
    terms = extracted_strings(case.expected_output)
    if case.inputs.source_file.endswith(".html"):
        marked = highlight_html(source, terms)
        source_html = (
            f'<iframe sandbox="" srcdoc="{html.escape(marked, quote=True)}"></iframe>'
        )
    else:
        source_html = f"<pre>{highlight_text(source, terms)}</pre>"
    expected_yaml = yaml.safe_dump(
        case.expected_output, sort_keys=False, allow_unicode=True
    )

    edits_html = ""
    if case.metadata.corrections:
        edits_html = (
            "<h3>Reviewer's corrections</h3><ul class='edits'>"
            + "".join(f"<li>{html.escape(e)}</li>" for e in case.metadata.corrections)
            + "</ul>"
        )

    status = "edited by reviewer" if case.metadata.edited else "accepted as extracted"
    return f"""
<section id="{html.escape(name)}">
  <h2>{html.escape(name)}</h2>
  <div class="meta">{len(items)} designees &middot; accepted {html.escape(case.metadata.accepted_at[:10])}
    &middot; {status}<br>{" ".join(links)}</div>
  <div>{rule_tags}{derived}</div>
  <div class="columns">
    <div><h3>{html.escape(case.inputs.source_label)}</h3>{source_html}</div>
    <div><h3>Accepted extraction</h3><pre>{html.escape(expected_yaml)}</pre></div>
  </div>
  {edits_html}
</section>"""


def render_dataset(
    dataset: FixtureDataset,
    fixtures_file: Path,
    tags_by_case: dict[str, set[str]],
    ide_link: str | None,
) -> str:
    lines = case_line_numbers(fixtures_file)
    counts = coverage_counts(tags_by_case)
    coverage_rows = "".join(
        f"<tr><td>{count}</td><td>{html.escape(tag)}</td></tr>"
        for tag, count in sorted(counts.items())
    )
    toc = "".join(
        f'<li><a href="#{html.escape(str(c.name))}">{html.escape(str(c.name))}</a>'
        f" <small>({len(c.expected_output['designees']) if c.expected_output else 0})</small></li>"
        for c in dataset.cases
    )
    sections = "".join(
        render_case(
            case,
            fixtures_file,
            lines.get(str(case.name)),
            tags_by_case.get(str(case.name), set()),
            ide_link,
        )
        for case in dataset.cases
    )
    title = html.escape(dataset.name or "")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title} fixtures</title><style>{STYLE}</style></head>
<body>
<header>
  <h1>{title}: {len(dataset.cases)} fixture cases</h1>
  <p>Fixtures file: <code>{html.escape(str(fixtures_file))}</code>. This page is for reviewing fixtures; edit them in that file.</p>
  <details><summary>Rule coverage</summary><table class="coverage">{coverage_rows}</table></details>
  <nav><ol>{toc}</ol></nav>
</header>
{sections}
</body></html>"""
