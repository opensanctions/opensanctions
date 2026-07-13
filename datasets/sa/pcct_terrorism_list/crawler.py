import re

from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

PROGRAM_NAME = "Saudi Arabia National Terrorism List (1373)"
PROGRAM_KEY = "SA-UNSC1373"

# An identifier part looks like a plain reference number: it starts with an
# alphanumeric, contains at least one digit and only uses a small set of
# separators. Anything else (prose, "A) ...; B) ..." markers) is kept as a note.
NUMBER_CLEAN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ./-]*$")

# Maps a normalized source document type to an FTM identifier property.
DOC_PROPS = {
    "passport": "passportNumber",
    "national identification": "idNumber",
    "national id": "idNumber",
    "residency number": "idNumber",
}


def clean_cell(value: str | None) -> str | None:
    """Strip surrounding whitespace and treat the literal 'NA' as empty."""
    if value is None:
        return None
    value = value.strip()
    if not value or value.upper() == "NA":
        return None
    return value


def split_values(value: str | None) -> list[str]:
    """Split a semicolon-separated multi-value cell into clean parts."""
    cleaned = clean_cell(value)
    if cleaned is None:
        return []
    return [p.strip() for p in cleaned.split(";") if p.strip()]


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
    entity: Entity,
    doc_type: str | None,
    doc_number: str | None,
    issuing_country: str | None,
) -> None:
    """Add identification numbers, keeping the raw value as a note.

    The source mixes clean reference numbers with free-text remarks and
    compound "A) ...; B) ..." entries, so structured properties are only set
    when the whole field parses cleanly. The raw text is always preserved.
    """
    doc_type = clean_cell(doc_type)
    doc_number = clean_cell(doc_number)
    issuing_country = clean_cell(issuing_country)
    if doc_number is None and doc_type is None:
        return

    note_parts = [p for p in (doc_type, doc_number) if p is not None]
    note = " — ".join(note_parts)
    if issuing_country is not None:
        note = f"{note} (issued in {issuing_country})"
    entity.add("notes", note)

    if doc_type is None:
        return
    prop = DOC_PROPS.get(" ".join(doc_type.lower().split()))
    numbers = parse_numbers(doc_number)
    if prop is not None and numbers:
        for number in numbers:
            entity.add(prop, number)


def emit_sanction(context: Context, entity: Entity, designated_date: str | None) -> None:
    sanction = h.make_sanction(
        context,
        entity,
        program_name=PROGRAM_NAME,
        source_program_key=PROGRAM_NAME,
        program_key=PROGRAM_KEY,
        start_date=clean_cell(designated_date),
    )
    entity.add("topics", "sanction")
    context.emit(sanction)


def crawl_individual(context: Context, row: dict[str, str | None]) -> None:
    name = clean_cell(row.pop("full_name"))
    if name is None:
        context.log.warning("Individual without a name", row=row)
        return
    designated_date = row.pop("designated_date")

    entity = context.make("Person")
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    for alias in split_values(row.pop("aliases")):
        entity.add("alias", alias)
    entity.add("gender", clean_cell(row.pop("gender")))
    h.apply_dates(entity, "birthDate", split_values(row.pop("dob")))
    for pob in split_values(row.pop("pob")):
        entity.add("birthPlace", pob)
    for cob in split_values(row.pop("cob")):
        entity.add("country", cob)
    for nationality in split_values(row.pop("nationality")):
        entity.add("nationality", nationality)
    for address in split_values(row.pop("address")):
        entity.add("address", address)
    for org in split_values(row.pop("link_of_terrorism_org")):
        entity.add("notes", f"Linked terrorist organization: {org}")
    entity.add("notes", clean_cell(row.pop("additional_information")))
    apply_documents(
        entity,
        row.pop("document_type"),
        row.pop("document_no"),
        row.pop("issuing_country"),
    )

    context.audit_data(row, ignore=["no"])
    emit_sanction(context, entity, designated_date)
    context.emit(entity)


def crawl_entity(context: Context, row: dict[str, str | None]) -> None:
    name = clean_cell(row.pop("entities_name"))
    if name is None:
        context.log.warning("Entity without a name", row=row)
        return
    designated_date = row.pop("designated_date")

    entity = context.make("Organization")
    entity.id = context.make_id(name, designated_date)
    entity.add("name", name)
    for alias in split_values(row.pop("aliases")):
        entity.add("alias", alias)
    for address in split_values(row.pop("address")):
        entity.add("address", address)
    for org in split_values(row.pop("link_of_terrorism_org")):
        entity.add("notes", f"Linked terrorist organization: {org}")

    registration = row.pop("registration_number")
    numbers = parse_numbers(registration)
    if numbers is None:
        entity.add("notes", clean_cell(registration))
    else:
        for number in numbers:
            entity.add("registrationNumber", number)

    owner_name = clean_cell(row.pop("owner_name"))
    owner_info = clean_cell(row.pop("owner_information"))
    if owner_name is not None:
        owner = context.make("LegalEntity")
        owner.id = context.make_id("owner", name, owner_name)
        owner.add("name", owner_name)
        owner.add("notes", owner_info)
        context.emit(owner)
        link = context.make("Ownership")
        link.id = context.make_id(entity.id, "owner", owner.id)
        link.add("owner", owner)
        link.add("asset", entity)
        context.emit(link)
    else:
        entity.add("notes", owner_info)
    entity.add("notes", clean_cell(row.pop("additional_info")))

    context.audit_data(row, ignore=["no"])
    emit_sanction(context, entity, designated_date)
    context.emit(entity)


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
    for org in split_values(row.pop("link_of_terrorism_org")):
        entity.add("notes", f"Linked terrorist organization: {org}")
    entity.add("notes", clean_cell(row.pop("additional_info")))

    context.audit_data(row, ignore=["no", "owner_name", "owner_information"])
    emit_sanction(context, entity, designated_date)
    context.emit(entity)


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
