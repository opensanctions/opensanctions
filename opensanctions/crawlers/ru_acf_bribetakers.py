import csv
from functools import cache
from pantomime.types import CSV
from normality import collapse_spaces, normalize

from opensanctions.core import Context
from opensanctions import helpers as h


@cache
def norm_tag(text):
    return normalize(text)


def parse_date(date):
    if h.check_no_year(date):
        return None
    return h.parse_date(date.strip(), ["%d.%m.%Y"])


def crawl_row(context: Context, row):
    entity = context.make("Person")
    tags = row.pop("Tag")
    name_en = row.pop("Name eng")
    dob = row.pop("DOB")
    entity.id = context.make_id(name_en, tags, dob)
    entity.add("name", name_en)
    entity.add("alias", row.get("Name cyrillic"))
    entity.add("birthDate", parse_date(dob))
    entity.add("gender", row.get("Gender"))
    description = collapse_spaces(row.get("Description"))
    result = context.lookup("descriptions", description)
    if result is not None:
        description = result.values
    entity.add("notes", description)
    for tag in tags.split("\n"):
        tag = tag.strip()
        ntag = norm_tag(tag)
        if ntag is None:
            continue
        if "oligarch" in ntag:
            entity.add("topics", "role.oligarch")
            continue
        elif "corrupt" in ntag:
            continue
        else:
            entity.add("position", tag)

    context.emit(entity, target=True)
    # context.inspect(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.source.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
