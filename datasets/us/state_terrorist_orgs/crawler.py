import re

from zavod import Context, helpers as h

NORMAL_CASE_RE = r"^(?P<name>[\w\s’'/-]+?)(?:\s*\((?P<alias>[\w\s’'/-]+?)\))?$"
PROGRAM = "Foreign Terrorist Organizations designated under section 219 of the INA"


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
    end_date = row.pop("date_removed", None)

    name_clean, name_former, alias = split_clean_name(context, name)
    entity = context.make("Organization")
    entity.id = context.make_id(name, start_date)
    entity.add("name", name_clean)
    entity.add("alias", alias)
    entity.add("previousName", name_former)
    entity.add("topics", ["sanction", "crime.terror"])

    sanction = h.make_sanction(context, entity)
    sanction.add("program", PROGRAM)
    h.apply_date(sanction, "startDate", start_date)
    if end_date:
        h.apply_date(sanction, "endDate", end_date)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)

    tables = doc.xpath(".//table")
    # We expect designated and delisted entities
    assert len(tables) == 2
    for table in tables:
        for row in h.parse_html_table(table):
            str_row = h.cells_to_str(row)
            crawl_row(context, str_row)
