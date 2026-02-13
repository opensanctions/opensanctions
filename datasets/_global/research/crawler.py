import csv
from normality import stringify
from typing import Dict, Optional
from rigour.mime.types import CSV
from followthemoney import model

from zavod import Context, Entity

SECURITIES_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtQD9wiHuyl23NmrIeAACET4OohOXhmuxQv817FHHas8uO4k8VBzex25nIOPqsG9300aXJIqCZzo--/pub?gid=0&single=true&output=csv"
BIDZINA_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtQD9wiHuyl23NmrIeAACET4OohOXhmuxQv817FHHas8uO4k8VBzex25nIOPqsG9300aXJIqCZzo--/pub?gid=351241481&single=true&output=csv"


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
    path = context.fetch_resource("source.csv", SECURITIES_CSV)
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

    path = context.fetch_resource("securities.csv", SECURITIES_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_sec_row(context, row)


def crawl_bidzina_row(context: Context, row: Dict):
    ult_owner = stringify(row.pop("UBO name"))
    ult_owner_id = stringify(row.pop("ID Number"))
    dir_owner = stringify(row.pop("Direct owner name (ENG)"))
    dir_owner_id = stringify(row.pop("Direct Owner ID"))
    percent = stringify(row.pop("Percentage"))
    company_name = stringify(row.pop("Company name (ENG)"))
    company_id = stringify(row.pop("Company ID"))
    company_jurisdiction = stringify(row.pop("Jurisdiction"))

    ult_owner_ent = context.make("Person")
    ult_owner_ent.id = context.make_id(ult_owner, ult_owner_id)

    company = context.make("Company")
    company.id = context.make_id(company_name, company_id)
    company.add("jurisdiction", company_jurisdiction)

    if ult_owner != dir_owner and ult_owner is not None:
        dir_company = context.make("Company")
        dir_company.id = context.make_id(dir_owner, dir_owner_id)
        ubo_ownership = context.make("Ownership")
        ubo_ownership.id = context.make_id(
            ult_owner, ult_owner_id, dir_owner, dir_owner_id
        )
        ubo_ownership.add("owner", ult_owner_ent.id)
        ubo_ownership.add("asset", dir_company.id)
    else:
        dir_company = context.make("Person")
        dir_company.id = context.make_id(dir_owner, dir_owner_id)

    dir_ownership = context.make("Ownership")
    dir_ownership.id = context.make_id(
        company_name, company_id, company_name, company_id
    )
    dir_ownership.add("owner", dir_company.id)
    dir_ownership.add("asset", company.id)
    dir_ownership.add("percent", percent)


def crawl_bidzina(context: Context):
    path = context.fetch_resource("source.csv", BIDZINA_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_bidzina_row(context, row)


def crawl(context: Context):
    # crawl_sec(context)
    crawl_bidzina(context)
