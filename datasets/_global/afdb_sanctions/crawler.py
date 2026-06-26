from zavod.extract.zyte_api import fetch_html

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    tables_xpath = ".//table[contains(@id, 'datatable')]"
    doc = fetch_html(
        context, context.data_url, tables_xpath, html_source="httpResponseBody"
    )
    table = h.xpath_element(doc, tables_xpath)
    for row in h.parse_html_table(table):
        cells = h.cells_to_str(row)
        if not any(cells.values()):  # Skip empty rows
            continue

        type_ = cells.pop("type")

        if all(v == "" for v in cells.values()):
            continue

        name = cells.pop("name")
        assert name is not None
        name = name.strip()
        country = cells.pop("nationality")
        entity_id = context.make_id(name, country)

        schema = context.lookup_value("types", entity_id)
        if schema is None:
            schema = context.lookup_value("types", type_)
        if schema is None:
            context.log.error("Unknown entity type", type=type_, item=cells)
            continue
        entity = context.make(schema)

        entity.id = entity_id
        h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
        entity.add("topics", "debarment")
        entity.add("country", country)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("basis"))
        h.apply_date(sanction, "startDate", cells.pop("from"))
        h.apply_date(sanction, "endDate", cells.pop("to"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(cells, ["notes", "debarment_from_date"])
