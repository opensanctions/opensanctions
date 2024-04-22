from zavod import Context, helpers as h
import datetime
import csv


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("Entity Name")
    description = input_dict.pop("Covered Equipment or Services")
    start_date = h.parse_date(
        input_dict.pop("Date of Inclusion on Covered List"), formats=["%b %d, %Y"]
    )

    entity = context.make("Company")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("topics", "sanction")

    for subsidiary_name in input_dict.pop("Subsidiary Names").split(","):
        if subsidiary_name == "":
            continue

        subsidiary = context.make("Company")
        subsidiary.id = context.make_slug(subsidiary_name)
        subsidiary.add("name", subsidiary_name)
        subsidiary.add("topics", "sanction")

        ownership = context.make("Ownership")
        ownership.id = context.make_slug(name, subsidiary_name)
        ownership.add("asset", subsidiary)
        ownership.add("percentage", "100%")
        ownership.add("owner", entity)

        subsidiary_sanction = h.make_sanction(context, subsidiary)

        subsidiary_sanction.add("description", description)
        subsidiary_sanction.add("startDate", start_date)

        context.emit(subsidiary, target=True)
        context.emit(ownership)
        context.emit(subsidiary_sanction)

    sanction = h.make_sanction(context, entity)

    sanction.add("description", description)
    sanction.add("startDate", start_date)

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    if datetime.datetime.now() > datetime.datetime(2024, 9, 19):
        context.log.warn("Check if there's an update of the data on the website.")

    data_path = context.dataset.base_path / "data.csv"

    for item in csv.DictReader(open(data_path, encoding="utf-8-sig"), delimiter=";"):
        crawl_item(item, context)
