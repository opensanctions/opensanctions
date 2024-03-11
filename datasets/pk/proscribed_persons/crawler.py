from zavod import Context
from zavod import helpers as h


def crawl_person(context: Context, row: dict):
    person_name = row.pop("name")
    father_name = row.pop("fatherName")
    cnic = row.pop("cnic")
    province = row.pop("province")
    district = row.pop("district")

    entity = context.make("Person")
    entity.id = context.make_slug(person_name, prefix="pk-proscribed")
    entity.add("idNumber", cnic)
    entity.add("name", person_name)
    entity.add("fatherName", father_name)
    entity.add("topics", "crime.terror")

    address_entity = h.make_address(
        context, state=province, region=district, country_code="pk"
    )
    entity.add("addressEntity", address_entity)
    context.emit(address_entity)

    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)

    for record in data:
        crawl_person(context, record)
