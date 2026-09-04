"""Turn accepted name-cleaning reviews into candidate DSPy examples.

Reviewers accept or correct the LLM's name cleaning in Data Reviews. The accepted
result is the closest thing we have to a gold standard, so it is the natural source
of new examples for ``single_entity_examples.yml``. This module reads the review
table, converts each accepted review into the example format, flags whether the
reviewer had to edit the LLM's output, and writes a report of what kinds of edits
were made so the examples the prompt most needs can be picked by hand.

No dspy import here, so it can be used and tested without the tuning stack.
"""

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import func, select

from zavod.db import get_engine
from zavod.extract.names.clean import Names
from zavod.stateful.model import review_table

EXAMPLE_FIELDS = ["name", "alias", "weakAlias", "previousName", "abbreviation"]
"""Names fields emitted in candidate examples. ``abbreviation`` is included even though
the DSPy signature does not produce it yet, so the gold data is ready when it does."""


@dataclass
class CandidateExample:
    dataset: str
    key: str
    reviewer: str
    entity_schema: str
    strings: list[str]
    expected: dict[str, list[str]]
    llm: dict[str, list[str]]
    edited: bool
    edit_kinds: list[str] = field(default_factory=list)

    def as_example(self) -> dict[str, Any]:
        example: dict[str, Any] = {
            "strings": self.strings,
            "entity_schema": self.entity_schema,
        }
        for prop in EXAMPLE_FIELDS:
            if self.expected.get(prop):
                example[prop] = self.expected[prop]
        return example

    def identity(self) -> tuple[str, tuple[str, ...]]:
        return example_identity(self.entity_schema, self.strings)


def example_identity(
    entity_schema: str, strings: Iterable[str]
) -> tuple[str, tuple[str, ...]]:
    return entity_schema, tuple(sorted(strings))


def _texts(names: Names) -> dict[str, list[str]]:
    """Field -> list of name texts, language tags dropped, empty fields omitted."""
    return {prop: [lt.text for lt in values] for prop, values in names.as_langtexts()}


def _strings_for_prompt(original: Names) -> list[str]:
    """The strings the LLM was given, in the same order as ``clean_names`` builds them."""
    strings: list[str] = []
    for _prop, values in original.as_langtexts():
        for value in values:
            if value.text not in strings:
                strings.append(value.text)
    return strings


def classify_edit(
    llm: dict[str, list[str]], expected: dict[str, list[str]]
) -> list[str]:
    """Rough labels for how the reviewer changed the LLM output, for the report."""

    def fold(text: str) -> str:
        return "".join(text.lower().split())

    llm_by_text = {t: prop for prop, ts in llm.items() for t in ts}
    exp_by_text = {t: prop for prop, ts in expected.items() for t in ts}
    kinds: set[str] = set()
    for text, prop in exp_by_text.items():
        if text in llm_by_text:
            if llm_by_text[text] != prop:
                kinds.add(f"recategorised {llm_by_text[text]} -> {prop}")
        elif any(fold(t) == fold(text) for t in llm_by_text):
            kinds.add("casing or spacing")
        elif any(text in t or t in text for t in llm_by_text):
            kinds.add("split, trimmed or merged")
        else:
            kinds.add("added by reviewer")
    for text in llm_by_text:
        if text not in exp_by_text and not any(
            fold(t) == fold(text) or text in t or t in text for t in exp_by_text
        ):
            kinds.add("dropped by reviewer")
    return sorted(kinds)


def review_to_candidate(
    dataset: str,
    key: str,
    reviewer: str,
    source_value: dict[str, Any],
    original_extraction: dict[str, Any],
    extracted_data: dict[str, Any],
) -> CandidateExample:
    """Convert one accepted review row into a candidate example."""
    original = Names.model_validate(source_value["original"])
    llm_names = Names.model_validate(original_extraction)
    expected_names = Names.model_validate(extracted_data)
    llm = _texts(llm_names)
    expected = _texts(expected_names)
    edited = llm_names != expected_names
    return CandidateExample(
        dataset=dataset,
        key=key,
        reviewer=reviewer,
        entity_schema=source_value["entity_schema"],
        strings=_strings_for_prompt(original),
        expected=expected,
        llm=llm,
        edited=edited,
        edit_kinds=classify_edit(llm, expected) if edited else [],
    )


def load_candidates(origin_like: str) -> list[CandidateExample]:
    """Accepted name reviews at each dataset's latest crawl version."""
    engine = get_engine()
    latest = (
        select(
            review_table.c.dataset,
            func.max(review_table.c.last_seen_version).label("version"),
        )
        .where(review_table.c.deleted_at.is_(None))
        .group_by(review_table.c.dataset)
        .subquery()
    )
    stmt = (
        select(review_table)
        .join(
            latest,
            (review_table.c.dataset == latest.c.dataset)
            & (review_table.c.last_seen_version == latest.c.version),
        )
        .where(
            review_table.c.deleted_at.is_(None),
            review_table.c.accepted.is_(True),
            review_table.c.source_label == "names",
            review_table.c.origin.like(origin_like),
        )
        .order_by(review_table.c.dataset, review_table.c.key)
    )
    candidates: list[CandidateExample] = []
    with engine.connect() as conn:
        for row in conn.execute(stmt).mappings():
            assert row["source_value"] is not None
            candidates.append(
                review_to_candidate(
                    dataset=row["dataset"],
                    key=row["key"],
                    reviewer=row["modified_by"].split("@")[0],
                    source_value=json.loads(row["source_value"]),
                    original_extraction=row["original_extraction"],
                    extracted_data=row["extracted_data"],
                )
            )
    return candidates


def existing_identities(examples_path: Path) -> set[tuple[str, tuple[str, ...]]]:
    with open(examples_path, encoding="utf-8") as fh:
        cases = yaml.load(fh, Loader=yaml.SafeLoader)
    return {example_identity(c["entity_schema"], c["strings"]) for c in cases}


class _Dumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> Any:
        return super().increase_indent(flow, False)


def write_candidates(
    candidates: list[CandidateExample],
    output_path: Path,
    report_path: Path,
    existing: set[tuple[str, tuple[str, ...]]],
) -> None:
    """Write candidates as YAML grouped by dataset, and a Markdown report.

    Examples already present in the examples file are left out of the YAML but
    counted in the report.
    """
    per_dataset: Counter[tuple[str, str]] = Counter()
    kinds: Counter[tuple[str, str]] = Counter()
    new: list[CandidateExample] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    for cand in candidates:
        per_dataset[(cand.dataset, "edited" if cand.edited else "accepted as-is")] += 1
        for kind in cand.edit_kinds:
            kinds[(cand.dataset, kind)] += 1
        if cand.identity() in existing or cand.identity() in seen:
            continue
        seen.add(cand.identity())
        new.append(cand)

    with open(output_path, "w", encoding="utf-8") as out:
        current_dataset = None
        for cand in new:
            if cand.dataset != current_dataset:
                current_dataset = cand.dataset
                out.write(f"\n# {cand.dataset}\n")
            note = (
                "reviewer edited: " + "; ".join(cand.edit_kinds)
                if cand.edited
                else "accepted as-is"
            )
            out.write(f"# {note} [{cand.reviewer}]\n")
            if cand.edited:
                out.write("# llm: " + json.dumps(cand.llm, ensure_ascii=False) + "\n")
            yaml.dump(
                [cand.as_example()],
                out,
                Dumper=_Dumper,
                allow_unicode=True,
                default_flow_style=None,
                sort_keys=False,
                width=1000,
            )

    with open(report_path, "w", encoding="utf-8") as rep:
        rep.write("# Accepted name reviews as candidate examples\n\n")
        rep.write(
            f"{len(candidates)} accepted reviews, {len(new)} new candidates written to {output_path.name}.\n\n"
        )
        rep.write("| dataset | accepted as-is | edited |\n|---|---|---|\n")
        for dataset in sorted({d for d, _ in per_dataset}):
            rep.write(
                f"| {dataset} | {per_dataset[(dataset, 'accepted as-is')]} "
                f"| {per_dataset[(dataset, 'edited')]} |\n"
            )
        rep.write(
            "\n## Kinds of reviewer edits\n\n| count | dataset | kind |\n|---|---|---|\n"
        )
        for (dataset, kind), count in kinds.most_common():
            rep.write(f"| {count} | {dataset} | {kind} |\n")
