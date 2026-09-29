"""Export accepted reviews from the review database as evaluation fixtures."""

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic_evals import Case
from sqlalchemy import select
from zavod.context import Context
from zavod.stateful.model import review_table
from zavod.stateful.review import model_hash

from contrib.prompt_evals.evaluators import ItemListMatch, describe_edits
from contrib.prompt_evals.models import CaseInputs, CaseMeta, Extraction, FixtureDataset

SOURCES_DIR = "sources"
ITEMS_KEY = "designees"
EVALUATOR_TYPES = [ItemListMatch]


def load_fixtures(path: Path) -> FixtureDataset:
    return FixtureDataset.from_file(path, custom_evaluator_types=EVALUATOR_TYPES)


def save_fixtures(path: Path, dataset: FixtureDataset) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_file(path, custom_evaluator_types=EVALUATOR_TYPES)


def case_name(url: str | None, key: str, taken: set[str]) -> str:
    """Name a case after the last path segment of the source URL where possible,
    so the report reads like a list of press releases rather than hashes."""
    name = key[:10]
    if url is not None:
        segment = url.rstrip("/").rsplit("/", 1)[-1]
        if segment:
            name = segment
    if name in taken:
        name = f"{name}-{key[:6]}"
    taken.add(name)
    return name


def normalised(data: Any, response_type: type[Any]) -> str:
    return model_hash(response_type.model_validate(data))


def accepted_reviews(
    context: Context, since: datetime | None, before: datetime | None
) -> list[dict[str, Any]]:
    """Reviews that are accepted, current, and were last modified in the given
    window. A review's `modified_at` is set when a reviewer accepts or edits it."""
    stmt = select(review_table).where(
        review_table.c.dataset == context.dataset.name,
        review_table.c.accepted.is_(True),
        review_table.c.deleted_at.is_(None),
    )
    if since is not None:
        stmt = stmt.where(review_table.c.modified_at >= since)
    if before is not None:
        stmt = stmt.where(review_table.c.modified_at < before)
    stmt = stmt.order_by(review_table.c.modified_at)
    return [dict(row._mapping) for row in context.db.execute(stmt).fetchall()]


def export_fixtures(
    context: Context,
    fixtures_file: Path,
    response_type: type[Any],
    since: datetime | None,
    before: datetime | None,
) -> tuple[int, int]:
    """Add accepted reviews as cases to the fixtures file, creating it if needed.
    Existing cases are kept and matched by review key. Returns (added, skipped)."""
    if fixtures_file.exists():
        dataset = load_fixtures(fixtures_file)
    else:
        dataset = FixtureDataset(
            name=context.dataset.name, cases=[], evaluators=[ItemListMatch()]
        )
    known_keys = {
        c.metadata.review_key for c in dataset.cases if c.metadata is not None
    }
    taken = {c.name for c in dataset.cases if c.name is not None}
    sources_dir = fixtures_file.parent / SOURCES_DIR
    sources_dir.mkdir(parents=True, exist_ok=True)

    added = skipped = 0
    for review in accepted_reviews(context, since, before):
        if review["key"] in known_keys:
            skipped += 1
            continue
        extracted: Extraction = review["extracted_data"]
        original: Extraction = review["original_extraction"]
        edited = normalised(extracted, response_type) != normalised(
            original, response_type
        )
        corrections = (
            describe_edits(
                list(extracted[ITEMS_KEY] or []), list(original[ITEMS_KEY] or [])
            )
            if edited
            else []
        )
        name = case_name(review["source_url"], review["key"], taken)
        suffix = ".html" if "html" in review["source_mime_type"] else ".txt"
        source_file = f"{SOURCES_DIR}/{name}{suffix}"
        (fixtures_file.parent / source_file).write_text(review["source_value"])
        dataset.cases.append(
            Case(
                name=name,
                inputs=CaseInputs(
                    source_file=source_file,
                    source_label=review["source_label"],
                    url=review["source_url"],
                ),
                expected_output=extracted,
                metadata=CaseMeta(
                    review_key=review["key"],
                    accepted_at=review["modified_at"].isoformat(),
                    edited=edited,
                    corrections=corrections,
                ),
            )
        )
        added += 1
    save_fixtures(fixtures_file, dataset)
    return added, skipped
