import os

import csv

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("姓名/Name")
    ethnicity = input_dict.pop("民族/Ethnicity")
    gender = input_dict.pop("性别/Gender")
    delegation = input_dict.pop("代表团/Delegation")

    entity = context.make("Person")
    entity.id = context.make_slug(name, ethnicity, gender, delegation)

    entity.add("name", name, lang="chi")
    entity.add("gender", context.lookup("gender", gender))
    entity.add("ethnicity", ethnicity, lang="chi")

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

    position.add("subnationalArea", delegation, lang="chi")

    if occupancy:
        context.emit(position)
        context.emit(entity, target=True)
        context.emit(occupancy)

    context.audit_data(input_dict)


def crawl(context: Context):
    dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(dir, "data.csv")

    with open(csv_file_path, "r") as fh:
        for item in csv.DictReader(fh):
            crawl_item(item, context)
