import csv
from normality import stringify
from typing import Dict, Optional
from pantomime.types import CSV
from followthemoney import model

from zavod import Context, Entity

SECURITIES_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtQD9wiHuyl23NmrIeAACET4OohOXhmuxQv817FHHas8uO4k8VBzex25nIOPqsG9300aXJIqCZzo--/pub?gid=0&single=true&output=csv"

def crawl_sec_row(context: Context, row: Dict[str, str]):
    entity = context.make("Company")
    name = row.pop("name")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("leiCode", row.pop("lei"))
    entity.add("permId", row.pop("perm_id"))

    security = context.make("Security")
    isin = row.pop("isin")
    security.id = f"isin-{isin}"
    security.add("isin", isin)
    security.add("issuer", entity)

    context.emit(entity)
    context.emit(security)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        entity: Optional[Entity] = None
        for row in csv.DictReader(fh):
            entity_id = stringify(row.pop('entity_id'))
            if entity_id is None:
                continue
            schema = stringify(row.pop('schema'))
            if schema is None or schema not in model.schemata:
                context.log.warn("Invalid schema", schema=schema, entity_id=entity_id)
                continue
            if entity is None or entity.id != row['id']:
                if entity is not None:
                    context.emit(entity)
                entity = context.make(schema)
                entity.id = entity_id
            prop = stringify(row.pop('prop'))
            if prop is None or prop not in entity.schema.properties:
                context.log.warn("Invalid property", prop=prop, entity_id=entity_id)
                continue
            value = stringify(row.pop('value'))
            original_value = stringify(row.pop('original_value'))
            lang = stringify(row.pop('lang'))
            entity.add(prop, value, original_value=original_value, lang=lang)
        
        if entity is not None:
            context.emit(entity)


    path = context.fetch_resource("securities.csv", SECURITIES_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_sec_row(context, row)
