from zavod import Context
from zavod import helpers as h


def crawl_person(context: Context, row: dict):
    person_name = row.pop("name")
    father_name = row.pop("fatherName")
    cnic = row.pop("cnic")
    province = row.pop("province")
    district = row.pop("district")

    entity = context.make("Person")

    if cnic:
        entity.id = context.make_slug(cnic, prefix="pk-cnic")
    else:
        entity.id = context.make_slug(person_name, district, province)

    name_split = person_name.split("@")
    if len(name_split) > 1:
        person_name = name_split[0]
        entity.add("alias", name_split[1:])

    entity.add("name", person_name)
    entity.add("idNumber", cnic)
    entity.add("fatherName", father_name)
    entity.add("topics", "sanction")
    entity.add(
        "address",
        f"{district}, {province}",
    )

    sanction = h.make_sanction(context, entity)
    sanction.add("program", "4th Schedule under the Anti Terrorism Act, 1997")

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)

    for record in data:
        crawl_person(context, record)
