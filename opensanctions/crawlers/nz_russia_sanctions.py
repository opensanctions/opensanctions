from datetime import datetime
import openpyxl
from openpyxl import Workbook
from banal import as_bool
from typing import Any, Dict
from pantomime.types import XLSX

from opensanctions.core import Context
from opensanctions import helpers as h

SCOPES = [
    "Aircraft Ban",
    "Asset Freeze",
    "Dealing with Securities",
    "Service Prohibition",
    "Ship Ban",
    "Travel Ban",
]
TYPES = {
    "Individual": "Person",
    "individual": "Person",
    "Entity": "LegalEntity",
    "Bank": "Company",
}


def parse_date(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return h.parse_date(value.strip(), ["%d/%m/%Y"])


def crawl_entity(context: Context, data: Dict[str, Any]) -> None:
    unique_id = data.pop("Unique Identifier")
    entity_type = data.pop("Type")
    if entity_type in ("Asset", "Total"):
        # Assets are not granular enough to be worth importing
        return None
    if entity_type not in TYPES:
        context.log.error("Unknown entity type", type=entity_type)
        return None
    entity = context.make(TYPES[entity_type])
    entity.id = context.make_slug(unique_id)
    entity.add("topics", "sanction")
    if entity_type == "Bank":
        entity.add("topics", "fin.bank")
    entity.add("notes", data.pop("Title"))
    entity.add("notes", data.pop("Additional Information", None))

    h.apply_name(
        entity,
        first_name=data.pop("First name"),
        middle_name=data.pop("Middle name(s)"),
        last_name=data.pop("Last name"),
        quiet=True,
    )

    aliases = data.pop("Alias/Alternate Spellings")
    if aliases is not None:
        entity.add("alias", aliases.split(";"))
    entity.add("address", data.pop("Address"))
    dob = parse_date(data.pop("DOB"))
    if entity.schema.is_a("Person"):
        entity.add("birthDate", dob)
    else:
        entity.add("incorporationDate", dob)
    entity.add("nationality", data.pop("Citizenship"), quiet=True)
    entity.add("nationality", data.pop("Citizenship 2"), quiet=True)
    entity.add("nationality", data.pop("Citizenship 3"), quiet=True)
    entity.add("birthPlace", data.pop("Place of Birth"), quiet=True)
    entity.add("passportNumber", data.pop("Passport Number"), quiet=True)

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", data.pop("Date of Sanction"))
    sanction.add("modifiedAt", data.pop("Date of Additional Sanction"))
    sanction.add("status", data.pop("Sanction Status"))
    sanction.add("reason", data.pop("General Rationale for Sanction"))
    for scope in SCOPES:
        if as_bool(data.pop(scope)):
            sanction.add("provisions", scope)

    context.emit(entity, target=True)
    context.emit(sanction)
    h.audit_data(data)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.source.data.url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook: Workbook = openpyxl.load_workbook(path, read_only=True)
    has_listing = False
    for sheet in workbook.worksheets:
        headers = None
        for row in sheet.rows:
            cells = [c.value for c in row]
            if "Unique Identifier" in cells and "DOB" in cells:
                headers = cells
                has_listing = True
                continue
            if headers is None:
                continue
            data = dict(zip(headers, cells))
            data.pop(None, None)
            crawl_entity(context, data)

    if not has_listing:
        context.log.error("Could not identify data sheet", sheets=workbook.sheetnames)
