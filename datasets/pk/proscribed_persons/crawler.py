from pathlib import Path
import shutil
import orjson
from rigour.mime.types import JSON
from stdnum.pk import cnic as cnic_validator  # type: ignore

from zavod import Context
from zavod import helpers as h

# 4th Schedule under the Anti Terrorism Act, 1997
PROGRAM_KEY = "PK-ATA1997"

LOCAL_PATH = Path(__file__).parent


def crawl_person(context: Context, row: dict):
    person_name = row.pop("Name")
    father_name = row.pop("FatherName")
    cnic = row.pop("CNIC")
    province = row.pop("Province")
    district = row.pop("District")

    entity = context.make("Person")
    if cnic_validator.is_valid(cnic):
        entity.id = context.make_slug(cnic, prefix="pk-cnic")
        entity.add("idNumber", cnic)
        entity.add("country", "pk")
    else:
        entity.id = context.make_slug(person_name, district, province)

    name_split = person_name.split("@")
    if len(name_split) > 1:
        person_name = name_split[0]
        entity.add("alias", name_split[1:])

    entity.add("name", person_name)
    entity.add("fatherName", father_name)
    entity.add("topics", "crime.terror")
    entity.add("topics", "wanted")
    entity.add("address", f"{district}, {province}")

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    source_path = LOCAL_PATH / "source.json"
    data = orjson.loads(source_path.read_bytes())
    # Export the source data as a resource by copying it from the dataset folder
    resource_path = context.get_resource_path("source.json")
    shutil.copy(source_path, resource_path)
    context.export_resource(resource_path, JSON, context.SOURCE_TITLE)

    for record in data:
        crawl_person(context, record)
