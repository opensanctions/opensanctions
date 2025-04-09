from zavod import Context, helpers as h

PROGRAM = "Foreign Terrorist Organizations designated under section 219 of the INA"


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)

    tables = doc.xpath(".//table")
    # We expect designated and delisted entities
    assert len(tables) == 2

    designated = tables[0]
    for row in h.parse_html_table(designated):
        str_row = h.cells_to_str(row)
        name = str_row.pop("name")
        start_date = str_row.pop("date_designated")

        if len(name.split(" (")) == 2:
            parts = name.split(" (")
            name_clean = parts[0]
            alias = parts[1].rstrip(")")
        else:
            name_clean = name
            print(name)
            alias = ""
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, start_date)
        entity.add("name", name_clean)
        entity.add("alias", alias)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", start_date)
        sanction.add("program", PROGRAM)

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(str_row)
