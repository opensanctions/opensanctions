import csv
from pathlib import Path
from typing import Dict
from pantomime.types import CSV

from zavod.context import Context
from zavod.helpers.addresses import make_address

LOCAL_PATH = Path(__file__).parent / "dataset.csv"


def crawl_row(context: Context, row: Dict[str, str]):
    schema = row.pop("type")
    entity = context.make(schema)
    entity.id = context.make_slug(row.pop("id"))
    entity.add("name", row.pop("name"))
    entity.add("alias", row.pop("alias"))
    entity.add("topics", row.pop("topics"))
    entity.add("notes", row.pop("notes"))

    address = make_address(
        context,
        street=row.pop("street"),
        city=row.pop("city"),
        postal_code=row.pop("postal_code"),
    )
    # h.apply_address(context, entity, address)
    entity.add("address", address.get("full"))

    if entity.schema.is_a("Person"):
        entity.add("nationality", row.pop("country"))
        entity.add("idNumber", row.pop("id_number"))
        entity.add("birthDate", row.pop("dob"))
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
        rel.add("startDate", row.pop("rel_start"))
        rel.add("endDate", row.pop("rel_end"))
        rel.add(rel.schema.source_prop, entity.id)
        rel.add(rel.schema.target_prop, other_id)
        context.emit(rel)

    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context):
    data_path = context.get_resource_path("source.csv")
    with open(LOCAL_PATH, "r") as fh:
        with open(data_path, "w") as out:
            out.write(fh.read())

    if context.dataset.data is not None and context.dataset.data.format == "FAIL":
        # Used by tests to trigger a runner failure.
        raise RuntimeError("Pipeline is broken")

    context.export_resource(data_path, CSV, title=context.SOURCE_TITLE)
    with open(data_path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    context.log.warn("This is a test warning")
