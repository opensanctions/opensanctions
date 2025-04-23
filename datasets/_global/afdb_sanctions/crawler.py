from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl(context: Context):
    next_url = context.data_url
    table_xpath = ".//table[contains(@class, 'views-table')]"
    while next_url:
        doc = fetch_html(context, next_url, table_xpath, cache_days=1)
        doc.make_links_absolute(next_url)
        next_link = doc.find(".//li[@class='next']/a")
        next_url = next_link.get("href") if next_link is not None else None
        tables = doc.xpath(table_xpath)
        assert len(tables) == 1, tables
        table = tables[0]
        for row in h.parse_html_table(table):
            cells = h.cells_to_str(row)
            type_ = cells.pop("type")
            schema = context.lookup_value("types", type_)
            if schema is None:
                context.log.error("Unknown entity type", type=type_)
                schema = "LegalEntity"

            name = cells.pop("name").strip()
            country = cells.pop("nationality")
            entity = context.make(schema)
            entity.id = context.make_id(name, country)
            entity.add("name", name)
            entity.add("country", country)

            sanction = h.make_sanction(context, entity)
            sanction.add("reason", cells.pop("basis"))
            h.apply_date(sanction, "startDate", cells.pop("from"))
            h.apply_date(sanction, "endDate", cells.pop("to"))
            if h.is_active(sanction):
                entity.add("topics", "debarment")

            context.emit(entity)
            context.emit(sanction)

            context.audit_data(cells, ["notes"])
