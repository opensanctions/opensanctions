import re
from typing import List
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context, Entity
from zavod import helpers as h


FORMATS = ["%d.%m.%Y"]

def parse_birth_dates(string: str) -> List[str]:
    strings = [collapse_spaces(s) for s in h.multi_split(string, ["si", ";"])]
    return [d for d in h.parse_date(s, FORMATS) for s in strings]

def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    table = doc.find(".//table")
    for row in h.parse_table(table):
        birth_dates = parse_birth_dates(row.pop("data-de-nastere"))
        schema = "LegalEntity" if birth_dates == [] else "Person"
        entity = context.make(schema)
        name = row.pop("persoana-fizica-entitate")
        entity.id = context.make_id(name, *birth_dates)
        