"""Which rules each fixture exercises, derived from its expected output and source
text, plus any hand-written `rules` slugs. Used to count coverage and to pick a
small subset of cases that still covers every rule."""

import re
from collections import Counter
from pathlib import Path

from lxml.html import HtmlElement, fromstring
from pydantic_evals import Case

from contrib.prompt_evals.evaluators import COMPARED_FIELDS
from contrib.prompt_evals.models import CaseInputs, CaseMeta, Extraction, FixtureDataset

LEGAL_FORM_RE = re.compile(
    r"\b(LLC|Ltd|Limited|Inc|Corp|Corporation|Co|S\.?A\.?|SAL|SARL|GmbH|AG|BV|NV|BVBA|"
    r"SPA|SRL|PLC|Pte|FZE|FZCO|DMCC|DOOEL|OU|OOO|AO|OAO|PAO|ZAO|JSC|OJSC|PJSC|Kft|"
    r"EOOD|SIA)\.?$"
)
ACRONYM_PAREN_RE = re.compile(r"[A-Za-z][\w'’ \-]+ \(([A-Z][A-Za-z\-]{1,10})\)")
QUOTED_NICKNAME_RE = re.compile(r"[\"“][A-Z][\w ]+[\"”] [A-Z]")
# Enforcement and government bodies that act in a press release but are never
# designees. A case where the source names one and the expected output does not
# demonstrates the bystander exclusion.
BYSTANDER_RE = re.compile(
    r"Financial Crimes Enforcement Network|FinCEN|Department of Justice|Federal Bureau "
    r"of Investigation|Drug Enforcement Administration|Homeland Security Investigations|"
    r"Internal Revenue Service|Europol|Department of State|Department of Commerce|"
    r"Secretariat of Security|National Crime Agency|Royal Canadian Mounted Police",
    re.IGNORECASE,
)


def source_text(
    fixtures_dir: Path, case: Case[CaseInputs, Extraction, CaseMeta]
) -> tuple[str, str]:
    """The source as (visible text, raw markup). Link targets only exist in the raw."""
    raw = (fixtures_dir / case.inputs.source_file).read_text()
    text = raw
    if case.inputs.source_file.endswith(".html"):
        element: HtmlElement = fromstring(raw)
        text = str(element.text_content())
    return re.sub(r"\s+", " ", text), raw


def derive_tags(
    case: Case[CaseInputs, Extraction, CaseMeta],
    text: str,
    raw: str,
    items_key: str = "designees",
) -> set[str]:
    """Rule slugs a case demonstrates, computed from expected output and source."""
    assert case.expected_output is not None
    items = list(case.expected_output.get(items_key) or [])
    tags: set[str] = set()
    for item in items:
        tags.add(f"schema-{item['entity_schema']}")
        if item["entity_schema"] == "Company" and LEGAL_FORM_RE.search(item["name"]):
            tags.add("schema-company-legal-form")
        for field in ("nationality", "imo", "country", "related_url"):
            if item.get(field):
                tags.add(f"field-{field}")
    if not items:
        tags.add("shape-0-designees")
    elif len(items) <= 2:
        tags.add(f"shape-{len(items)}-designees")
    elif len(items) >= 15:
        tags.add("shape-large-batch")
    if ACRONYM_PAREN_RE.search(text):
        tags.add("name-acronym-in-source")
    if QUOTED_NICKNAME_RE.search(text):
        tags.add("name-quoted-nickname-in-source")
    if re.search(r"\b(vessel|tanker)s?\b", text):
        tags.add("vessel-in-source")
    if "identified as blocked property" in text:
        tags.add("vessel-blocked-property")
    if re.search(r"Department of State is (also |concurrently )?designating", text):
        tags.add("state-dept-codesignation")
    if "recent-actions" in raw:
        tags.add("related-url-recent-actions-in-source")
    if BYSTANDER_RE.search(text):
        if any(BYSTANDER_RE.search(item["name"]) for item in items):
            tags.add("bystander-agency-accepted-as-designee")
        else:
            tags.add("bystander-agency-excluded")
    if case.metadata is not None:
        for correction in case.metadata.corrections:
            kind = correction.split(":", 1)[0]
            if kind in ("added", "removed", "renamed"):
                tags.add(f"edit-{kind}")
            for field in COMPARED_FIELDS:
                if f" {field}: " in correction:
                    tags.add(f"edit-{field}")
        tags.update(case.metadata.rules)
    return tags


def tag_cases(dataset: FixtureDataset, fixtures_dir: Path) -> dict[str, set[str]]:
    return {
        str(case.name): derive_tags(case, *source_text(fixtures_dir, case))
        for case in dataset.cases
    }


def coverage_counts(tags_by_case: dict[str, set[str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for tags in tags_by_case.values():
        counts.update(tags)
    return counts


def select_cases(
    dataset: FixtureDataset,
    tags_by_case: dict[str, set[str]],
    target: int,
    always: set[str],
    max_items: int | None = None,
) -> list[str]:
    """Greedily choose the fewest cases such that every tag is covered by at least
    `target` cases (or by all cases carrying it, if fewer exist).

    One case per source URL is kept, preferring a case named in `always`, then
    the most recently accepted. Ties prefer edited cases, then cases with fewer
    items (cheaper to review). Cases with more than `max_items` items are only
    used when named in `always`, since every item must be human-reviewed.
    """
    by_name = {str(c.name): c for c in dataset.cases}

    def accepted_at(name: str) -> str:
        meta = by_name[name].metadata
        assert meta is not None
        return meta.accepted_at

    newest_per_url: dict[str, str] = {}
    for name, case in by_name.items():
        key = case.inputs.url or name
        current = newest_per_url.get(key)
        if current in always:
            continue
        if (
            name in always
            or current is None
            or accepted_at(name) > accepted_at(current)
        ):
            newest_per_url[key] = name
    pool = set(newest_per_url.values())
    if max_items is not None:
        pool = {
            n
            for n in pool
            if n in always or len(by_name[n].expected_output["designees"]) <= max_items  # type: ignore[index]
        }
    counts = coverage_counts({n: tags_by_case[n] for n in pool})
    need: Counter[str] = Counter({t: min(target, c) for t, c in counts.items()})

    chosen: list[str] = [n for n in always if n in pool]
    for name in chosen:
        need.subtract(tags_by_case[name])
    remaining = pool - set(chosen)
    while any(v > 0 for v in need.values()):

        def gain(name: str) -> tuple[int, int, int]:
            case = by_name[name]
            assert case.expected_output is not None and case.metadata is not None
            useful = sum(1 for t in tags_by_case[name] if need[t] > 0)
            return (
                useful,
                int(case.metadata.edited),
                -len(case.expected_output["designees"]),
            )

        best = max(remaining, key=gain)
        if gain(best)[0] == 0:
            break
        chosen.append(best)
        remaining.remove(best)
        need.subtract(tags_by_case[best])
    return chosen
