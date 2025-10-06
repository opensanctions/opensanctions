from stdnum.pk import cnic as cnic_validator  # type: ignore

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_json

# 4th Schedule under the Anti Terrorism Act, 1997
PROGRAM_KEY = "PK-ATA1997"


def crawl_person(context: Context, row: dict):
    person_name = row.pop("name")
    father_name = row.pop("fatherName")
    cnic = row.pop("cnic")
    province = row.pop("province")
    district = row.pop("district")

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
    data = fetch_json(context, context.data_url, cache_days=1, geolocation="PK")

    for record in data:
        crawl_person(context, record)
