import re
from typing import List, Tuple
from lxml import html
from rigour.mime.types import HTML

from zavod import Context
from zavod import helpers as h

SPLITS = [
    "si",
    ";",
    "sau",
    "a)",
    "b)",
    "c)",
    "d)",
    "Aproximativ ",
    "Intre",
    "între",
    "și",
    "la",
    "din pasaport fals",
    "presupusă:",
]


def clean_name(string: str):
    name = re.sub(r"^[\]\), ]+", "", string)
    name = re.sub(r"[\[\(\., ]+$", "", string)
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
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        dob = str_row.pop("data_de_nastere")
        entity = context.make("LegalEntity")
        name, aliases = parse_names(str_row.pop("persoana_fizica_entitate"))
        entity.id = context.make_id(name, dob)
        entity.add("name", name)
        entity.add("topics", "sanction")
        if aliases:
            entity.add("alias", aliases)
        for date in h.multi_split(dob, SPLITS):
            entity.add_schema("Person")
            h.apply_date(entity, "birthDate", date)

        sanction = h.make_sanction(context, entity)
        sanction.add("program", str_row.pop("sanctiuni_teroriste") or None, lang="mol")
        sanction.add(
            "program", str_row.pop("sanctiuni_de_proliferare") or None, lang="mol"
        )

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(str_row)
