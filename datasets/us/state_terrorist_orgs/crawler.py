import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

NORMAL_CASE_RE = r"^(?P<name>[\w\s’'/-]+?)(?:\s*\((?P<alias>[\w\s’'/-]+?)\))?$"
# Foreign Terrorist Organizations designated under section 219 of the INA
PROGRAM_KEY = "US-FTO219"


def split_clean_name(context, name):
    name = name.strip()
    name_former = None
    alias = None

    match = re.fullmatch(NORMAL_CASE_RE, name)
    if match:
        return match.group("name").strip(), name_former, match.group("alias")

    # Override lookup for known complex cases
    result = context.lookup("names", name)
    if result and result.names:
        override = result.names[0]
        return (
            override.get("name_clean"),
            override.get("name_former"),
            override.get("alias"),
        )

    context.log.warning("Name override is not found", name=name)

    return name, name_former, alias


def crawl_row(context, row):
    name = row.pop("name")
    start_date = row.pop("date_designated", None) or row.pop(
        "date_originally_designated", None
    )
    # Rely on auditing rows to be sure the default of None doesn't mean we miss these
    # if the column name changes.
    end_date = row.pop("date_removed", None)

    name_clean, name_former, alias = split_clean_name(context, name)
    entity = context.make("Organization")
    entity.id = context.make_id(name, start_date)
    entity.add("name", name_clean)
    entity.add("alias", alias)
    entity.add("previousName", name_former)

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    h.apply_date(sanction, "startDate", start_date)
    if end_date:
        h.apply_date(sanction, "endDate", end_date)
    if h.is_active(sanction):
        entity.add("topics", ["sanction", "crime.terror"])

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = fetch_html(context, context.data_url, ".//table")

    tables = doc.xpath(".//table")
    # We expect designated and delisted entities
    assert len(tables) == 2
    for table in tables:
        for row in h.parse_html_table(table):
            str_row = h.cells_to_str(row)
            crawl_row(context, str_row)
