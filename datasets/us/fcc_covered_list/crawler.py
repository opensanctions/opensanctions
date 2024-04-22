import csv
from pantomime.types import CSV

from zavod import Context, helpers as h


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("Entity Name")
    description = input_dict.pop("Covered Equipment or Services")
    start_date = h.parse_date(
        input_dict.pop("Date of Inclusion on Covered List"), formats=["%b %d, %Y"]
    )

    entity = context.make("Company")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("topics", "debarment")

    for subsidiary_name in input_dict.pop("Subsidiary Names").split(";"):
        subsidiary_name = subsidiary_name.strip()
        if subsidiary_name == "":
            continue

        subsidiary = context.make("Company")
        subsidiary.id = context.make_slug(subsidiary_name)
        subsidiary.add("name", subsidiary_name)
        subsidiary.add("topics", "debarment")

        ownership = context.make("Ownership")
        ownership.id = context.make_id(name, subsidiary_name)
        ownership.add("asset", subsidiary)
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
    h.assert_url_hash(
        context,
        context.dataset.url,
        "c89f87926db3e1794109f1cd0128f554358bd180",
    )

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path) as fh:
        for item in csv.DictReader(fh):
            crawl_item(item, context)
