import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h

US_FCC = "US-FCC"
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en;q=0.9",
    "Priority": "u=0, i",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("Entity Name")
    description = input_dict.pop("Covered Equipment or Services")

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
        h.apply_date(
            subsidiary_sanction,
            "startDate",
            input_dict.get("Date of Inclusion on Covered List"),
        )

        context.emit(subsidiary)
        context.emit(ownership)
        context.emit(subsidiary_sanction)

    sanction = h.make_sanction(context, entity, program_key=US_FCC)
    sanction.add("description", description)
    sanction.add("description", input_dict.pop("Notes 1"))
    sanction.add("description", input_dict.pop("Notes 2"))
    h.apply_date(
        sanction, "startDate", input_dict.pop("Date of Inclusion on Covered List")
    )

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.url, headers=HEADERS)
    table = doc.xpath('.//div[contains(@class, "page-body")]//table')[0]
    h.assert_dom_hash(
        table,
        "964c2ad2036c92380cfeb4eb8254e281666a4dbe",
    )

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path) as fh:
        for item in csv.DictReader(fh):
            crawl_item(item, context)
