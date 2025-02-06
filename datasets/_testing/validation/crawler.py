import csv
from typing import Dict
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    schema = row.pop("type")
    entity = context.make(schema)
    entity.id = context.make_slug(row.pop("id"))
    entity.add("name", row.pop("name"))
    entity.add("alias", row.pop("alias"))
    entity.add("topics", row.pop("topics"))
    entity.add("notes", row.pop("notes"))

    address = h.make_address(
        context,
        street=row.pop("street"),
        city=row.pop("city"),
        postal_code=row.pop("postal_code"),
        country=row.get("country"),
    )
    h.apply_address(context, entity, address)

    if entity.schema.is_a("Person"):
        entity.add("nationality", row.pop("country"))
        entity.add("idNumber", row.pop("id_number"))
        h.apply_date(entity, "birthDate", row.pop("dob"))
    else:
        entity.add("jurisdiction", row.pop("country"))
        entity.add("registrationNumber", row.pop("id_number"))
        entity.add("incorporationDate", row.pop("dob"))

    rel_schema = row.pop("rel_type")
    if rel_schema is not None and len(rel_schema.strip()):
        rel = context.make(rel_schema)
        other_id = context.make_slug(row.pop("rel_other"))
        rel.id = context.make_id("rel", entity.id, other_id)
        rel.add("summary", row.pop("rel_summary"))
        h.apply_date(rel, "startDate", row.pop("rel_start"))
        h.apply_date(rel, "endDate", row.pop("rel_end"))
        rel.add(rel.schema.source_prop, entity.id)
        rel.add(rel.schema.target_prop, other_id)
        context.emit(rel)

    context.emit(entity)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
