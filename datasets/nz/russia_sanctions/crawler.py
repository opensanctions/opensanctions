import openpyxl
from openpyxl import Workbook
from banal import as_bool
from normality import stringify
from datetime import datetime, time
from typing import Any, Dict, List, Optional
from pantomime.types import XLSX

from zavod import Context, Entity
from zavod import helpers as h

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
ASSOCIATE = ("associate", "close associate")
FAMILY = ("relative", "wife", "son", "daughter", "nephew")


def parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, time)):
        return None
    return h.parse_date(value.strip(), ["%d/%m/%Y"])


def parse_associates(context: Context, target: Entity, value: str) -> None:
    splits = value.split(" of", 1)
    if len(splits) != 2:
        context.log.warning("Cannot parse associate info", value=value)
        return
    link, other = splits
    other = other.strip().strip("'")
    entity = context.make("LegalEntity")
    entity.id = context.make_slug("named", other)
    entity.add("name", other)
    context.emit(entity)
    if link.lower().strip() in ASSOCIATE:
        rel = context.make("UnknownLink")
        rel.id = context.make_id(target.id, entity.id, value)
        rel.add("subject", target)
        rel.add("object", entity)
        rel.add("role", value)
        context.emit(rel)
    elif link.lower().strip() in FAMILY:
        rel = context.make("Family")
        rel.id = context.make_id(target.id, entity.id, value)
        rel.add("person", target)
        rel.add("relative", entity)
        rel.add("relationship", value)
        context.emit(rel)
    else:
        context.log.warning("Unknown associate link type", value=value)


def crawl_entity(context: Context, data: Dict[str, Any]) -> None:
    unique_id = data.pop("Unique Identifier")
    entity_type = data.pop("Type").strip()
    if entity_type in ("Asset", "Total"):
        # Assets are not granular enough to be worth importing
        return None
    if entity_type not in TYPES:
        context.log.error("Unknown entity type", type=entity_type)
        return None
    entity = context.make(TYPES[entity_type])
    first_name = data.pop("First name")
    middle_name = data.pop("Middle name(s)")
    last_name = data.pop("Last name")
    entity.id = context.make_slug(unique_id, first_name, last_name, strict=False)
    entity.add("topics", "sanction")
    if entity_type == "Bank":
        entity.add("topics", "fin.bank")
    entity.add("notes", data.pop("Title"))
    entity.add("notes", data.pop("Additional Information", None))

    h.apply_name(
        entity,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
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

    associates = data.pop("Associates/Relatives", None)
    if associates is not None:
        parse_associates(context, entity, associates)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", "Russia Sanctions Act 2022")
    sanction.add("startDate", data.pop("Date of Sanction"))
    sanction.add("modifiedAt", parse_date(data.pop("Date of Additional Sanction")))
    sanction.add("status", data.pop("Sanction Status"))
    sanction.add("reason", data.pop("General Rationale for Sanction"))
    for scope in SCOPES:
        if as_bool(data.pop(scope)):
            sanction.add("provisions", scope)

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(data)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook: Workbook = openpyxl.load_workbook(path, read_only=True)
    has_listing = False
    for sheet in workbook.worksheets:
        headers: Optional[List[Optional[str]]] = None
        for row in sheet.rows:
            cells = [c.value for c in row]
            if "Unique Identifier" in cells and "DOB" in cells:
                headers = [stringify(h) for h in cells]
                has_listing = True
                continue
            if headers is None:
                continue
            data = dict(zip(headers, cells))
            data_ = {k: v for k, v in data.items() if k is not None}
            crawl_entity(context, data_)

    if not has_listing:
        context.log.error("Could not identify data sheet", sheets=workbook.sheetnames)
