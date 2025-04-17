from zavod import Context, helpers as h

PROGRAM = "Foreign Terrorist Organizations designated under section 219 of the INA"


def split_clean_name(context, name):
    # Defaults
    name_clean = name.strip()
    name_former = None
    alias = None
    # Lookup override if name contains a dash
    if any(dash in name for dash in ["—", "–"]):
        result = context.lookup("names", name)
        if not result or not result.names:
            context.log.warning("Name override is not found", name=name)
            return name_clean, name_former, alias
        for name in result.names:
            name_clean = name.get("name_clean")
            name_former = name.get("name_former")
            alias = name.get("alias")
    # Parse inline alias format: e.g., "Organization Name (Alias)"
    elif len(name.split(" (")) == 2:
        base, alias_part = name.rsplit(" (", 1)
        name_clean = base.strip()
        alias = alias_part.rstrip(")").strip()
        name_former = None

    return name_clean, name_former, alias


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
