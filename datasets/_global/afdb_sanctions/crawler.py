from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl(context: Context):
    tables_xpath = ".//table[contains(@id, 'datatable')]"
    doc = fetch_html(
        context, context.data_url, tables_xpath, html_source="httpResponseBody"
    )
    # We expect 2 tables (firms and individuals)
    tables = doc.xpath(tables_xpath)
    assert len(tables) == 2, f"Expected 2 tables, got {len(tables)}"
    for table in tables:
        for row in h.parse_html_table(table):
            cells = h.cells_to_str(row)

            # AfDB lists several individuals as firms in places where the IADB
            # shows them to be people (and they have normal personal names)

            # type_ = cells.pop("type")
            # schema = context.lookup_value("types", type_)
            # if schema is None:
            #     context.log.error("Unknown entity type", type=type_)
            #     continue

            name = cells.pop("name").strip()
            if all(v == "" for v in cells.values()):
                continue
            country = cells.pop("nationality")
            entity = context.make("LegalEntity")
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

            context.audit_data(cells, ["type", "notes"])
