import re
from typing import Literal, NamedTuple, Optional

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

# A plain reference number: starts with an alphanumeric, contains at least one
# digit and only uses a small set of separators. Anything else (prose,
# "A) ...; B) ..." markers) is treated as unparseable and kept as a note.
NUMBER_CLEAN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ./-]*$")

# Maps the extracted document type to the FTM identifier property it populates.
DOCUMENT_PROPS = {
    "passport": "passportNumber",
    "nationalId": "idNumber",
    "residency": "idNumber",
}


class IdentityDocument(BaseModel):
    """A single identity document extracted from the source document field."""

    type: Literal["passport", "nationalId", "residency", "other"] = Field(
        description=(
            "The kind of document: 'passport', 'nationalId' for a national "
            "identification number, 'residency' for a residency permit or number, "
            "or 'other' for anything else."
        )
    )
    number: str = Field(
        description="A single document number, exactly as written in the source."
    )
    country: Optional[str] = Field(
        default=None, description="The issuing country, if stated."
    )


class IdentityDocuments(BaseModel):
    documents: list[IdentityDocument] = Field(default_factory=list)


DOCUMENT_PROMPT = """
Extract every identity document from the text of a sanctions designation. The
text combines a document type, one or more document numbers, and an issuing
country. A single entry may list several numbers separated by semicolons, or
compound entries like "A) National identification; B) Passport" that pair each
number with a type. Return one item per distinct document number.

Instructions for specific fields:
  - type: passport, nationalId (national identification), residency (residency
    permit/number), or other.
  - number: a single document number, exactly as written in the source.
  - country: the issuing country, if stated.
"""


class Relation(NamedTuple):
    """Shape of a related entity and the edge that links it to the anchor."""

    schema: str  # schema of the related entity
    link_schema: str  # schema of the edge entity
    anchor_prop: str  # edge property pointing at the anchor entity
    related_prop: str  # edge property pointing at the related entity
    role: str  # the edge's role, describing the relationship
    topics: str | None  # optional topic set on the related entity


RELATIONS = {
    # The terrorist organization each designation is affiliated with.
    "terror-org": Relation(
        schema="Organization",
        link_schema="UnknownLink",
        anchor_prop="subject",
        related_prop="object",
        role="Linked terrorist organization",
        topics="crime.terror",
    ),
    # The owner of a designated organization or vessel.
    "owner": Relation(
        schema="LegalEntity",
        link_schema="Ownership",
        anchor_prop="asset",
        related_prop="owner",
        role="Owner",
        topics=None,
    ),
}


def clean_cell(value: str | None) -> str | None:
    """Strip surrounding whitespace and treat the literal 'NA' as empty."""
    if value is None:
        return None
    value = value.strip()
    if not value or value.upper() == "NA":
        return None
    return value


def apply_addresses(context: Context, entity: Entity, value: str | None) -> None:
    """Compose each semicolon-separated address and copy it onto the entity.

    The source publishes addresses as single strings (e.g. "Beirut, Lebanon"),
    so they go in as ``full=`` and ``make_address`` parses out the country.
    """
    for full in h.multi_split(value, [";"]):
        address = h.make_address(context, full=full)
        h.copy_address(entity, address)


def parse_numbers(value: str | None) -> list[str] | None:
    """Split an identifier field into plain reference numbers.

    Returns the list of numbers if every part looks like a plain identifier,
    or ``None`` if the field contains prose the caller should keep as a note.
    """
    cleaned = clean_cell(value)
    if cleaned is None:
        return []
    numbers: list[str] = []
    for part in cleaned.split(";"):
        # Drop bracketed annotations like "(Turkey)" before validating.
        unbracketed = (h.remove_bracketed(part) or "").strip()
        if not unbracketed:
            continue
        if not NUMBER_CLEAN.match(unbracketed):
            return None
        if not any(char.isdigit() for char in unbracketed):
            return None
        numbers.append(unbracketed)
    return numbers


def apply_documents(
    context: Context,
    entity: Entity,
    doc_type: str | None,
    doc_number: str | None,
    issuing_country: str | None,
) -> None:
    """Extract identity documents and add the confirmed ones to the entity.

    The document columns mix clean numbers, free-text remarks and compound
    "A) ...; B) ..." entries, so parsing is delegated to an LLM whose output is
    confirmed through the human review system before any number is emitted.
    """
    doc_type = clean_cell(doc_type)
    doc_number = clean_cell(doc_number)
    issuing_country = clean_cell(issuing_country)
    if doc_number is None:
        return

    labelled = [
        f"{label}: {value}"
        for label, value in (
            ("Document type", doc_type),
            ("Document number", doc_number),
            ("Issuing country", issuing_country),
        )
        if value is not None
    ]
    text = "\n".join(labelled)
    source_value = TextSourceValue(
        key_parts=text,
        label="Identity documents",
        text=text,
        url=context.data_url,
    )
    extraction = run_typed_text_prompt(
        context,
        prompt=DOCUMENT_PROMPT,
        string=source_value.value_string,
        response_type=IdentityDocuments,
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=extraction,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return
    for document in review.extracted_data.documents:
        prop = DOCUMENT_PROPS.get(document.type)
        if prop is not None:
            entity.add(prop, document.number, origin=review.origin)


def emit_sanction(
    context: Context, entity: Entity, designated_date: str | None
) -> None:
    sanction = h.make_sanction(
        context,
        entity,
        program_key=PROGRAM_KEY,
        start_date=designated_date,
    )
    entity.add("topics", "sanction")
    context.emit(sanction)


def emit_related(
    context: Context,
    entity: Entity,
    kind: str,
    name: str | None,
    notes: str | None = None,
) -> None:
    """Emit a related entity of the given ``kind`` and the edge linking it.

    Handles both the affiliated terrorist organization and the owner (see
    ``RELATIONS``). The related entity is keyed on its name alone, so the same
    organization or owner referenced by several designations shares one entity;
    duplicates are resolved at the later dedup stage. When no name is present,
    any remark stays on the anchor entity.
    """
    relation = RELATIONS[kind]
    name = clean_cell(name)
    notes = clean_cell(notes)
    if name is None:
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


def crawl_individual(context: Context, row: dict[str, str | None]) -> None:
    name = row.pop("full_name")
    designated_date = row.pop("designated_date")

    entity = context.make("Person")
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    for alias in h.multi_split(row.pop("aliases"), [";"]):
        entity.add("alias", alias)
    entity.add("gender", clean_cell(row.pop("gender")))
    h.apply_dates(entity, "birthDate", h.multi_split(row.pop("dob"), [";"]))
    for pob in h.multi_split(row.pop("pob"), [";"]):
        entity.add("birthPlace", pob)
    for cob in h.multi_split(row.pop("cob"), [";"]):
        entity.add("country", cob)
    for nationality in h.multi_split(row.pop("nationality"), [";"]):
        entity.add("nationality", nationality)
    apply_addresses(context, entity, row.pop("address"))
    for org in h.multi_split(row.pop("link_of_terrorism_org"), [";"]):
        emit_related(context, entity, "terror-org", org)
    entity.add("notes", clean_cell(row.pop("additional_information")))
    apply_documents(
        context,
        entity,
        row.pop("document_type"),
        row.pop("document_no"),
        row.pop("issuing_country"),
    )

    emit_sanction(context, entity, designated_date)
    context.emit(entity)

    context.audit_data(row, ignore=["no"])


def crawl_entity(context: Context, row: dict[str, str | None]) -> None:
    name = clean_cell(row.pop("entities_name"))
    if name is None:
        context.log.warning("Entity without a name", row=row)
        return
    designated_date = row.pop("designated_date")

    entity = context.make("Organization")
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    for alias in h.multi_split(row.pop("aliases"), [";"]):
        entity.add("alias", alias)
    apply_addresses(context, entity, row.pop("address"))
    for org in h.multi_split(row.pop("link_of_terrorism_org"), [";"]):
        emit_related(context, entity, "terror-org", org)

    registration = row.pop("registration_number")
    numbers = parse_numbers(registration)
    if numbers is None:
        entity.add("notes", clean_cell(registration))
    else:
        for number in numbers:
            entity.add("registrationNumber", number)

    emit_related(
        context, entity, "owner", row.pop("owner_name"), row.pop("owner_information")
    )
    entity.add("notes", clean_cell(row.pop("additional_info")))

    emit_sanction(context, entity, designated_date)
    context.emit(entity)

    context.audit_data(row, ignore=["no"])


def crawl_vessel(context: Context, row: dict[str, str | None]) -> None:
    name = clean_cell(row.pop("vessel_name"))
    imo = clean_cell(row.pop("imo_no"))
    if name is None and imo is None:
        context.log.warning("Vessel without a name or IMO number", row=row)
        return
    designated_date = row.pop("designated_date")

    entity = context.make("Vessel")
    entity.id = context.make_id("vessel", imo, name)
    entity.add("name", name)
    if imo is not None:
        # Values are prefixed, e.g. "IMO 9105085".
        entity.add("imoNumber", imo.replace("IMO", "").strip())

    flag = clean_cell(row.pop("vessel_flag"))
    if flag is not None:
        # Values look like "Iranian flag"; strip the suffix before country cleaning.
        entity.add("flag", re.sub(r"\bflag\b", "", flag, flags=re.I).strip())
    for org in h.multi_split(row.pop("link_of_terrorism_org"), [";"]):
        emit_related(context, entity, "terror-org", org)
    emit_related(
        context, entity, "owner", row.pop("owner_name"), row.pop("owner_information")
    )
    entity.add("notes", clean_cell(row.pop("additional_info")))

    emit_sanction(context, entity, designated_date)
    context.emit(entity)

    context.audit_data(row, ignore=["no"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = load_workbook(path, read_only=True)

    for row in h.parse_xlsx_sheet(context, workbook["Individuals"]):
        crawl_individual(context, row)
    for row in h.parse_xlsx_sheet(context, workbook["Entities "]):
        crawl_entity(context, row)
    for row in h.parse_xlsx_sheet(context, workbook["Vessels"]):
        crawl_vessel(context, row)

    # Document extractions await human review; warn rather than block the whole
    # dataset, since the documents are a secondary enrichment here.
    assert_all_accepted(context, raise_on_unaccepted=False)
