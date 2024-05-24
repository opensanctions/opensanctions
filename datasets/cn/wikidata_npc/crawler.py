import csv
from pantomime.types import CSV

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("姓名")
    ethnicity = input_dict.pop("民族")
    gender = input_dict.pop("性别")
    birth_date = input_dict.pop("出生日期")

    entity = context.make("Person")
    entity.id = context.make_id(name, ethnicity, gender, birth_date)

    entity.add("name", name, lang="chi")
    entity.add("gender", gender)
    entity.add("ethnicity", ethnicity, lang="chi")
    entity.add(
        "birthDate", h.parse_date(birth_date, formats=["%Y年%m月"])
    )
    entity.add("political", input_dict.pop("政党"))

    position = h.make_position(
        context, "Member of the National People’s Congress", country="cn"
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    occupancy.add("description", input_dict.pop("职务"))
    occupancy.add("description", input_dict.pop("备注"))

    if occupancy:
        context.emit(position)
        context.emit(entity, target=True)
        context.emit(occupancy)

    context.audit_data(input_dict)


def crawl(context: Context):

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for i, item in enumerate(csv.DictReader(fh)):
            context.log.info(i)
            crawl_item(item, context)
