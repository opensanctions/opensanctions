from typing import NamedTuple

from openpyxl import load_workbook
from rigour.mime.types import XLSX
from zavod.entity import Entity

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "SA-UNSC1373"


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
        "Organization",
        "UnknownLink",
        "subject",
        "object",
        "Linked terrorist organization",
        "crime.terror",
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


def crawl_row(context: Context, schema: str, row: dict[str, str | None]) -> None:
    """
    All three sheets share this path; the headers are normalized to a common
    vocabulary by the ``columns`` lookup, and properties that don't apply to a
    given schema are added with ``quiet=True`` and silently skipped.
    """
    name = row.pop("name")
    if clean_cell(name) is None:
        context.log.warning("Row without a name", row=row)
        return
    designated_date = row.pop("designated_date")
    birth_date = row.pop("birth_date", None)
    doc_type = row.pop("doc_type", None)

    entity = context.make(schema)
    # Strict rule: only raw source values go into the ID.
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    if entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(birth_date, [";"]))
    entity.add("alias", h.multi_split(row.pop("alias", None), [";"]), quiet=True)
    entity.add("gender", row.pop("gender", None), quiet=True)
    entity.add(
        "birthPlace", h.multi_split(row.pop("birth_place", None), [";"]), quiet=True
    )
    entity.add(
        "nationality", h.multi_split(row.pop("nationality", None), [";"]), quiet=True
    )
    entity.add(
        "country", h.multi_split(row.pop("birth_country", None), [";"]), quiet=True
    )
    entity.add("flag", row.pop("flag", None), quiet=True)
    entity.add("imoNumber", row.pop("imo_number", None), quiet=True)
    entity.add("notes", clean_cell(row.pop("notes", None)))
    entity.add(
        "registrationNumber",
        h.multi_split(row.pop("reg_number", None), [";"]),
        quiet=True,
    )
    doc_type = row.pop("doc_type", None)
    if doc_type is not None:
        passport = h.make_identification(
            context,
            entity,
            row.pop("doc_number", None),
            doc_type,
            country=row.pop("issuing_country", None),
            passport=(doc_type == "Passport"),
        )
        if passport is not None:
            context.emit(passport)

    for full in h.multi_split(row.pop("address", None), [";"]):
        entity.add("address", full)
    # Emit linked organizations
    for org in h.multi_split(row.pop("linked_org", None), [";"]):
        emit_related(context, entity, "terror-org", org)
    # Emit owners of designated vessels
    emit_related(
        context,
        entity,
        "owner",
        row.pop("owner_name", None),
        row.pop("owner_info", None),
    )

    sanction = h.make_sanction(
        context,
        entity,
        program_key=PROGRAM_KEY,
        start_date=designated_date,
    )

    context.emit(entity)
    context.emit(sanction)

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
