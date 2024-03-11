from zavod import Context
from zavod import helpers as h


def crawl_person(context: Context, row: dict):
    person_name = row.pop("name")
    father_name = row.pop("fatherName")
    cnic = row.pop("cnic")
    province = row.pop("province")
    district = row.pop("district")

    entity = context.make("Person")
    entity.id = context.make_slug(
        cnic or f"{person_name} - {district}, {province}", prefix="pk-cnic"
    )
    name_split = person_name.split("@")
    if len(name_split) > 1:
        person_name = name_split[0]
        for alias in name_split[1:]:
            entity.add("alias", alias)

    entity.add("name", person_name)
    entity.add("idNumber", cnic)
    entity.add("fatherName", father_name)
    entity.add("topics", "sanction")

    address_entity = h.make_address(
        context, state=province, region=district, full=f"{district}, {province}"
    )
    entity.add("addressEntity", address_entity)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", "4th Schedule under the Anti Terrorism Act, 1997")

    context.emit(entity, target=True)
    context.emit(address_entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)

    for record in data:
        crawl_person(context, record)
