import csv
from normality import stringify
from typing import Dict, Optional
from rigour.mime.types import CSV
from followthemoney import model

from zavod import Context, Entity
from zavod import helpers as h

SECURITIES_STATEMENTS_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=1612308021&single=true&output=csv"
SECURITIES_CUSTOM_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=0&single=true&output=csv"
SANCTION_OWNERSHIP_CSV = "https://docs.google.com/spreadsheets/d/1Mi5HzeUuWpQ4XrNk8JS7KF0-JKTaUjiNEnDpT0om4mc/pub?gid=351241481&single=true&output=csv"

IGNORE_FIELDS: list[str] = [
    "As of",
]


def add_source_link(entity: Entity, link: str) -> None:
    if entity.id is not None and link is not None and link.startswith("http"):
        entity.add("sourceUrl", link)


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


def crawl_sanction_ownership_row(context: Context, row: Dict):
    # source link to prove the relationships when a link exists
    source_link = stringify(row.pop("Source Link"))
    owner_schema = stringify(row.pop("Owner Schema"))

    # company
    company_ent = context.make("Company")
    company_name_eng = stringify(row.pop("Company name (ENG)"))
    company_id = stringify(row.pop("Company ID"))
    company_ent.id = context.make_id(company_name_eng, company_id)
    company_name_geo = stringify(row.pop("Company name (GEO)"))
    h.apply_name(company_ent, full=company_name_eng, name_prop="name", lang="eng")
    h.apply_name(company_ent, full=company_name_geo, name_prop="name", lang="geo")
    company_ent.add("registrationNumber", company_id)
    company_jurisdiction = stringify(row.pop("Jurisdiction"))
    company_ent.add("jurisdiction", company_jurisdiction)
    context.emit(company_ent)

    # owner of company
    if owner_schema == "Company":
        owner_ent = context.make("Company")
    elif owner_schema == "Person":
        owner_ent = context.make("Person")
    owner_name_eng = stringify(row.pop("Direct owner name (ENG)"))
    owner_id = stringify(row.pop("Direct Owner ID"))
    owner_ent.id = context.make_id(owner_name_eng, owner_id)
    h.apply_name(owner_ent, full=owner_name_eng, lang="eng")
    owner_name_geo = stringify(row.pop("Direct owner name (GEO)"))
    h.apply_name(owner_ent, full=owner_name_geo, lang="geo")
    owner_ent.add("registrationNumber", owner_id)
    context.emit(owner_ent)

    # company ownership
    company_ownership = context.make("Ownership")
    company_ownership.id = context.make_id(
        company_name_eng, company_id, owner_name_eng, owner_id
    )
    company_ownership.add("owner", owner_ent.id)
    company_ownership.add("asset", company_ent.id)
    percent = stringify(row.pop("Percentage"))
    company_ownership.add("percentage", percent)
    add_source_link(company_ownership, source_link)
    context.emit(company_ownership)

    # add ubo if exists
    ubo_name = stringify(row.pop("UBO name"))
    if ubo_name:
        ubo_ent = context.make("Person")
        ubo_id = stringify(row.pop("ID Number"))
        ubo_ent.id = context.make_id(ubo_name, ubo_id)
        h.apply_name(ubo_ent, full=ubo_name, lang="eng")
        ubo_ent.add("idNumber", ubo_id)
        context.emit(ubo_ent)

        ubo_ownership = context.make("Ownership")
        ubo_ownership.id = context.make_id(ubo_name, ubo_id, owner_name_eng, owner_id)
        ubo_ownership.add("owner", ubo_ent.id)
        ubo_ownership.add("asset", owner_ent.id)
        add_source_link(ubo_ownership, source_link)
        context.emit(ubo_ownership)


def crawl_sanction_ownership(context: Context):
    path = context.fetch_resource(
        "sanction-ownership-source.csv", SANCTION_OWNERSHIP_CSV
    )
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_sanction_ownership_row(context, row)


def crawl(context: Context):
    # crawl_sec(context)
    crawl_sanction_ownership(context)
