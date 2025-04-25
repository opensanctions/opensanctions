from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


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

        type_ = cells.pop("type")
        schema = context.lookup_value("types", type_)
        if schema is None:
            context.log.error("Unknown entity type", type=type_)
            continue

        name = cells.pop("name").strip()
        if all(v == "" for v in cells.values()):
            continue
        country = cells.pop("nationality")
        entity = context.make(schema)
        entity.id = context.make_id(name, country)
        entity.add("name", name)
        entity.add("topics", "debarment")
        entity.add("country", country)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("basis"))
        h.apply_date(sanction, "startDate", cells.pop("from"))
        h.apply_date(sanction, "endDate", cells.pop("to"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(cells, ["notes", "debarment_from_date"])
