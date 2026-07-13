from typing import Literal, NamedTuple

from openpyxl import load_workbook
from pydantic import BaseModel, Field
from rigour.mime.types import XLSX
from zavod.entity import Entity
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "SA-UNSC1373"


class Identifier(BaseModel):
    """A single identity or registration number extracted from the source."""

    type: Literal["passport", "nationalId", "residency", "registration", "other"] = (
        Field(
            description=(
                "The kind of number: 'passport', 'nationalId' (national "
                "identification), 'residency' (residency permit/number), "
                "'registration' (company registration number) or 'other'."
            )
        )
    )
    number: str = Field(
        description="A single number, exactly as written in the source."
    )


class Identifiers(BaseModel):
    identifiers: list[Identifier] = Field(default_factory=list)


IDENTIFIER_PROMPT = """
Extract every identity or registration number from the text of a sanctions
designation. The text combines a label, one or more numbers, and sometimes an
issuing country. A single entry may list several numbers separated by
semicolons, or compound entries like "A) National identification; B) Passport"
that pair each number with a type. Return one item per distinct number.

Instructions for specific fields:
  - type: passport, nationalId (national identification), residency (residency
    permit/number), registration (company registration number) or other.
  - number: a single number, exactly as written in the source.
"""


class Relation(NamedTuple):
    """How a related entity and the edge linking it to the anchor are shaped."""

    schema: str  # schema of the related entity
    link_schema: str  # schema of the edge entity
    anchor_prop: str  # edge property pointing at the anchor entity
    related_prop: str  # edge property pointing at the related entity
    role: str  # the edge's role, describing the relationship
    topics: str | None  # optional topic set on the related entity


RELATIONS = {
    # The terrorist organization each designation is affiliated with.
    "terror-org": Relation(
        "Organization", "UnknownLink", "subject", "object",
        "Linked terrorist organization", "crime.terror",
    ),
    # The owner of a designated organization or vessel.
    "owner": Relation("LegalEntity", "Ownership", "asset", "owner", "Owner", None),
}


def clean_cell(value: str | None) -> str | None:
    """Strip surrounding whitespace and treat the literal 'NA' as empty."""
    if value is None:
        return None
    value = value.strip()
    if not value or value.upper() == "NA":
        return None
    return value


def emit_related(
    context: Context,
    entity: Entity,
    kind: str,
    name: str | None,
    notes: str | None = None,
) -> None:
    """Emit a related entity of the given ``kind`` and the edge linking it.

    Handles both the affiliated terrorist organization and the owner (see
    ``RELATIONS``). The related entity is keyed on its raw name, so the same
    organization or owner referenced by several designations shares one entity;
    duplicates are resolved at the later dedup stage. When no name is present,
    any remark stays on the anchor entity.
    """
    relation = RELATIONS[kind]
    notes = clean_cell(notes)
    if clean_cell(name) is None:
        entity.add("notes", notes)
        return

    related = context.make(relation.schema)
    related.id = context.make_id(kind, name)
    related.add("name", name)
    related.add("topics", relation.topics)
    related.add("notes", notes)
    context.emit(related)

    link = context.make(relation.link_schema)
    link.id = context.make_id(entity.id, kind, related.id)
    link.add(relation.anchor_prop, entity)
    link.add(relation.related_prop, related)
    link.add("role", relation.role)
    context.emit(link)


def apply_identifiers(context: Context, entity: Entity, row: dict[str, str | None]) -> None:
    """Extract identity and registration numbers via reviewed LLM extraction.

    The document and registration columns mix clean numbers, free-text remarks
    and compound "A) ...; B) ..." entries, so parsing is delegated to an LLM
    whose output is confirmed through the human review system before any number
    is emitted. Each number's type is mapped to a property via the
    ``identifier.type`` lookup; quiet adds drop numbers that don't fit the
    entity's schema.
    """
    labelled = [
        f"{label}: {value}"
        for label, value in (
            ("Document type", clean_cell(row.pop("doc_type", None))),
            ("Document number", clean_cell(row.pop("doc_number", None))),
            ("Issuing country", clean_cell(row.pop("issuing_country", None))),
            ("Registration number", clean_cell(row.pop("reg_number", None))),
        )
        if value is not None
    ]
    if not labelled:
        return

    text = "\n".join(labelled)
    source_value = TextSourceValue(
        key_parts=text, label="Identifiers", text=text, url=context.data_url
    )
    extraction = run_typed_text_prompt(
        context,
        prompt=IDENTIFIER_PROMPT,
        string=source_value.value_string,
        response_type=Identifiers,
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=extraction,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return
    for identifier in review.extracted_data.identifiers:
        prop = context.lookup_value("identifier.type", identifier.type)
        if prop is not None:
            entity.add(prop, identifier.number, origin=review.origin, quiet=True)


def crawl_row(context: Context, schema: str, row: dict[str, str | None]) -> None:
    """Emit a designated entity of ``schema`` from a source row.

    All three sheets share this path; the headers are normalized to a common
    vocabulary by the ``columns`` lookup, and properties that don't apply to a
    given schema are added with ``quiet=True`` and silently skipped.
    """
    name = row.pop("name")
    if clean_cell(name) is None:
        context.log.warning("Row without a name", row=row)
        return
    designated_date = row.pop("designated_date")

    entity = context.make(schema)
    # Strict rule: only raw source values go into the ID.
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    entity.add("alias", h.multi_split(row.pop("alias", None), [";"]), quiet=True)
    entity.add("gender", clean_cell(row.pop("gender", None)), quiet=True)
    entity.add("birthPlace", h.multi_split(row.pop("birth_place", None), [";"]), quiet=True)
    entity.add("nationality", h.multi_split(row.pop("nationality", None), [";"]), quiet=True)
    entity.add("country", h.multi_split(row.pop("birth_country", None), [";"]), quiet=True)
    entity.add("flag", clean_cell(row.pop("flag", None)), quiet=True)
    entity.add("imoNumber", clean_cell(row.pop("imo_number", None)), quiet=True)
    entity.add("notes", clean_cell(row.pop("notes", None)))

    birth_date = row.pop("birth_date", None)
    if entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(birth_date, [";"]))

    for full in h.multi_split(row.pop("address", None), [";"]):
        h.copy_address(entity, h.make_address(context, full=full))
    for org in h.multi_split(row.pop("linked_org", None), [";"]):
        emit_related(context, entity, "terror-org", org)
    emit_related(
        context, entity, "owner", row.pop("owner_name", None), row.pop("owner_info", None)
    )
    apply_identifiers(context, entity, row)

    sanction = h.make_sanction(
        context, entity, program_key=PROGRAM_KEY, start_date=designated_date
    )
    entity.add("topics", "sanction")
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row, ignore=["no"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = load_workbook(path, read_only=True)

    columns = context.get_lookup("columns")
    for sheet in workbook.sheetnames:
        schema = context.lookup_value("schema", sheet)
        if schema is None:
            context.log.warning("Unmapped sheet", sheet=sheet)
            continue
        for row in h.parse_xlsx_sheet(context, workbook[sheet], header_lookup=columns):
            crawl_row(context, schema, row)

    # Identifier extractions await human review; warn rather than block the whole
    # dataset, since the numbers are a secondary enrichment here.
    assert_all_accepted(context, raise_on_unaccepted=False)
