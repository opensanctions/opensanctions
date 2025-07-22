import re

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

NAME_SPLITS = [
    "doing business as",
    "also known as",
    " or ",
    "a.k.a.",
    "aka ",
    "f/k/a",
    "d/b/a",
    "dba",
    "d.b.a.",
    "a/k/a",
    "operating",
    "formerly",
    "previously",
]
DETECT_ALIAS_RE = re.compile("|".join(NAME_SPLITS), re.IGNORECASE)
NAME_REGEX = re.compile(r"^(?P<name>[^()]+?)\s*\(\s*(?P<alias>[^()]+?)\s*\)$")


def apply_clean_name(context: Context, entity, name):
    names_lookup_result = context.lookup("names", name)
    if names_lookup_result is not None:
        details = names_lookup_result.names[0]
        entity.add("name", details.get("name"))
        entity.add("alias", details.get("alias"))
        entity.add("previousName", details.get("previous_name"))
    # Only allow the normal "Name (Alias)" format if no alias-y terms were found
    elif NAME_REGEX.match(name) and not DETECT_ALIAS_RE.search(name):
        match = NAME_REGEX.match(name)
        entity.add("name", match.group("name").strip())
        entity.add("alias", match.group("alias").strip('“”"'))
    # If the name looks like it might contain an alias, log a warning
    elif "(" in name or DETECT_ALIAS_RE.search(name):
        context.log.warn(
            "Name looks like it might contain an alias, but no lookup found", name=name
        )
    else:
        entity.add("name", name)


def crawl(context: Context):
    tables_xpath = ".//table[contains(@id, 'datatable')]"
    doc = fetch_html(
        context, context.data_url, tables_xpath, html_source="httpResponseBody"
    )
    tables = doc.xpath(tables_xpath)
    assert len(tables) == 1
    table = tables[0]
    for row in h.parse_html_table(table):
        cells = h.cells_to_str(row)
        if not any(cells.values()):  # Skip empty rows
            continue

        type_ = cells.pop("type")
        schema = context.lookup_value("types", type_)
        if schema is None:
            context.log.error("Unknown entity type", type=type_, item=cells)
            continue

        name = cells.pop("name").strip()
        if all(v == "" for v in cells.values()):
            continue
        country = cells.pop("nationality")
        entity = context.make(schema)
        entity.id = context.make_id(name, country)
        apply_clean_name(context, entity, name)
        entity.add("topics", "debarment")
        entity.add("country", country)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("basis"))
        h.apply_date(sanction, "startDate", cells.pop("from"))
        h.apply_date(sanction, "endDate", cells.pop("to"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(cells, ["notes", "debarment_from_date"])
