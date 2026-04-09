import csv
from normality import stringify
from typing import Dict, Optional
from rigour.mime.types import CSV
from followthemoney import model

from zavod import Context, Entity

SECURITIES_STATEMENTS_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=1612308021&single=true&output=csv"
SECURITIES_CUSTOM_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=0&single=true&output=csv"
IVANISHVILI_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=351241481&single=true&output=csv"

IGNORE_FIELDS: list[str] = [
    "Direct owner name (GEO)",
    "As of",
    "Company name (GEO)",
    "Source Link",
]


def crawl_sec_row(context: Context, row: Dict[str, str]):
    entity = context.make("Company")
    name = row.pop("name")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("leiCode", row.pop("lei"))
    entity.add("permId", row.pop("perm_id"))

    if isin := row.pop("isin", None):
        security = context.make("Security")
        security.id = f"isin-{isin}"
        security.add("isin", isin)
        security.add("issuer", entity)
        context.emit(security)

    context.emit(entity)
    context.audit_data(row)


def crawl_sec(context: Context):
    path = context.fetch_resource("sec-source.csv", SECURITIES_STATEMENTS_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        entity: Optional[Entity] = None
        for row in csv.DictReader(fh):
            entity_id = stringify(row.pop("entity_id"))
            if entity_id is None:
                continue
            schema = stringify(row.pop("schema"))
            if schema is None or schema not in model.schemata:
                context.log.warn("Invalid schema", schema=schema, entity_id=entity_id)
                continue
            if entity is None or entity.id != entity_id:
                if entity is not None:
                    context.emit(entity)
                entity = context.make(schema)
                entity.id = entity_id
            prop = stringify(row.pop("prop"))
            if prop is None or prop not in entity.schema.properties:
                context.log.warn("Invalid property", prop=prop, entity_id=entity_id)
                continue
            value = stringify(row.pop("value"))
            original_value = stringify(row.pop("original_value"))
            lang = stringify(row.pop("lang"))
            entity.add(prop, value, original_value=original_value, lang=lang)

        if entity is not None:
            context.emit(entity)

    path = context.fetch_resource("securities.csv", SECURITIES_CUSTOM_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_sec_row(context, row)


def crawl_ivanishvili_row(context: Context, row: Dict):
    ult_owner_name = stringify(row.pop("UBO name"))
    ult_owner_id = stringify(row.pop("ID Number"))
    dir_owner_name = stringify(row.pop("Direct owner name (ENG)"))
    dir_owner_id = stringify(row.pop("Direct Owner ID"))
    percent = stringify(row.pop("Percentage"))
    company_name = stringify(row.pop("Company name (ENG)"))
    company_id = stringify(row.pop("Company ID"))
    company_jurisdiction = stringify(row.pop("Jurisdiction"))

    ult_owner_ent = context.make("Person")
    ult_owner_ent.id = context.make_id(ult_owner_name, ult_owner_id)
    ult_owner_ent.add("name", ult_owner_name)
    ult_owner_ent.add("idNumber", ult_owner_id)

    company_ent = context.make("Company")
    company_ent.id = context.make_id(company_name, company_id)
    company_ent.add("name", company_name)
    company_ent.add("registrationNumber", company_id)
    company_ent.add("jurisdiction", company_jurisdiction)

    # if the ult_owner is different, then the dir_company is a company
    # otherwise, the dir_company is the ubo (a person) and directly owns
    # main company
    if ult_owner_name is None:
        dir_owner_ent = context.make("Company")
        dir_owner_ent.id = context.make_id(dir_owner_name, dir_owner_id)
        dir_owner_ent.add("name", dir_owner_name)
        dir_owner_ent.add("registrationNumber", dir_owner_id)
    elif ult_owner_name != dir_owner_name and ult_owner_name is not None:
        dir_owner_ent = context.make("Company")
        dir_owner_ent.id = context.make_id(dir_owner_name, dir_owner_id)
        dir_owner_ent.add("name", dir_owner_name)
        dir_owner_ent.add("registrationNumber", dir_owner_id)
        ult_ownership = context.make("Ownership")
        ult_ownership.id = context.make_id(
            ult_owner_name, ult_owner_id, dir_owner_name, dir_owner_id
        )
        ult_ownership.add("owner", ult_owner_ent.id)
        ult_ownership.add("asset", dir_owner_ent.id)

        context.emit(ult_owner_ent)
        context.emit(dir_owner_ent)
        context.emit(ult_ownership)
    else:
        dir_owner_ent = context.make("Person")
        dir_owner_ent.id = context.make_id(dir_owner_name, dir_owner_id)
        dir_owner_ent.add("name", dir_owner_name)
        dir_owner_ent.add("registrationNumber", dir_owner_id)

    dir_ownership = context.make("Ownership")
    dir_ownership.id = context.make_id(
        company_name, company_id, dir_owner_name, dir_owner_id
    )
    dir_ownership.add("owner", dir_owner_ent.id)
    dir_ownership.add("asset", company_ent.id)
    dir_ownership.add("percentage", percent)

    context.emit(dir_owner_ent)
    context.emit(company_ent)
    context.emit(dir_ownership)
    context.audit_data(row, IGNORE_FIELDS)


def crawl_ivanisvhili(context: Context):
    path = context.fetch_resource("bi-source.csv", IVANISHVILI_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_ivanishvili_row(context, row)


def crawl(context: Context):
    crawl_sec(context)
    crawl_ivanisvhili(context)
