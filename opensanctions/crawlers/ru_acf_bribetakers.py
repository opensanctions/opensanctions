import csv
from pantomime.types import CSV
from normality import collapse_spaces

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    if h.check_no_year(date):
        return None
    return h.parse_date(date.strip(), ["%d.%m.%Y"])


def crawl_row(context: Context, row):
    entity = context.make("Person")
    tag = row.pop("Tag")
    name_en = row.pop("Name eng")
    dob = row.pop("DOB")
    entity.id = context.make_id(name_en, tag, dob)
    entity.add("name", name_en)
    entity.add("alias", row.get("Name cyrillic"))
    entity.add("birthDate", parse_date(dob))
    entity.add("notes", collapse_spaces(row.get("Description")))
    entity.add("position", tag.split("\n"))
    entity.add("gender", row.get("Gender"))

    context.emit(entity, target=True)
    # context.pprint(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
