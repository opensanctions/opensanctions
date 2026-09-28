"""Render one dataset's fixtures as a static HTML page for quick human review:
each case shows the source document beside the accepted extraction, what the
reviewer changed, and where the case lives in the fixtures file."""

import html
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic_evals import Case

from contrib.prompt_evals.coverage import COMPARED_FIELDS, coverage_counts
from contrib.prompt_evals.evaluators import norm_value, pair_items
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


def case_line_numbers(fixtures_file: Path) -> dict[str, int]:
    """Line of each `- name:` entry in the fixtures file, for jumping to it in an editor."""
    lines: dict[str, int] = {}
    for number, line in enumerate(fixtures_file.read_text().splitlines(), start=1):
        match = re.match(r"^- name: (.+)$", line)
        if match:
            lines[match.group(1).strip().strip("'\"")] = number
    return lines


def describe_edits(
    expected: list[dict[str, Any]], original: list[dict[str, Any]], key: str = "name"
) -> list[str]:
    """Human-readable list of what the reviewer changed between the model's
    original extraction and the accepted extraction."""
    pairs, missing, spurious = pair_items(expected, original, key)
    edits = [f"added: {name}" for name in missing]
    edits += [f"removed: {name}" for name in spurious]
    for exp, orig in pairs:
        if exp[key] != orig[key]:
            edits.append(f"renamed: {orig[key]!r} -> {exp[key]!r}")
        for field in COMPARED_FIELDS:
            if norm_value(exp.get(field)) != norm_value(orig.get(field)):
                edits.append(
                    f"{exp[key]} {field}: {orig.get(field)!r} -> {exp.get(field)!r}"
                )
    return edits


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
    if case.inputs.source_file.endswith(".html"):
        source_html = (
            f'<iframe sandbox="" srcdoc="{html.escape(source, quote=True)}"></iframe>'
        )
    else:
        source_html = f"<pre>{html.escape(source)}</pre>"
    expected_yaml = yaml.safe_dump(
        case.expected_output, sort_keys=False, allow_unicode=True
    )

    edits_html = ""
    if case.metadata.edited and case.metadata.original_extraction is not None:
        original = case.metadata.original_extraction.get("designees") or []
        edits = describe_edits(items, original)
        edits_html = (
            "<h3>Reviewer's corrections</h3><ul class='edits'>"
            + "".join(f"<li>{html.escape(e)}</li>" for e in edits)
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
