import re
from typing import List, Tuple
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context
from zavod import helpers as h


FORMATS = ["%d.%m.%Y"]
SPLITS = ["si", ";", "sau", "a)", "b)", "c)"]


def parse_birth_dates(string: str) -> List[str]:
    strings = h.multi_split(string, SPLITS)
    # flatten
    return [date for s in strings for date in h.parse_date(s, FORMATS)]


def clean_name(string: str):
    name = re.sub("^[\]\), ]+", "", string)
    name = re.sub("[\[\(\., ]+$", "", string)
    return name

def parse_names(string: str) -> Tuple[str, List[str]]:
    parts = string.split("alias")
    name = clean_name(parts[0])
    aliases = parts[1:] if len(parts) > 1 else []
    aliases = [clean_name(alias) for alias in aliases]
    return name, aliases


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
        name, aliases = parse_names(row.pop("persoana-fizica-entitate"))
        entity.id = context.make_id(name, *sorted(birth_dates))
        entity.add("name", name)
        if aliases:
            entity.add("alias", aliases)
        if birth_dates:
            entity.add("birthDate", birth_dates)

        sanction = h.make_sanction(context, entity)
        sanction.add("program", row.pop("sanctiuni-teroriste") or None, lang="mol")
        sanction.add("program", row.pop("sanctiuni-de-proliferare") or None, lang="mol")

        context.emit(entity, target=True)
        context.emit(sanction)

        context.audit_data(row)
