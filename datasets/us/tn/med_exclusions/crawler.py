from typing import Dict

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(row: Dict[str, str], context: Context):
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    npi = row.pop("npi")

    if first_name:
        entity = context.make("Person")
        entity.id = context.make_id(last_name, first_name, npi)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, npi)
        entity.add("name", last_name)

    entity.add("npiCode", npi)
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("reason", row.pop("reason"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator=".//table[@id='DataTables_Table_0']",
        geolocation="us",
    )
    table = doc.xpath(".//table[@id='DataTables_Table_0']")
    assert len(table) == 1, f"Expected 1 table, got {len(table)}"
    table = table[0]
    for row in h.parse_html_table(doc.find(".//table")):
        str_row = h.cells_to_str(row)
        crawl_item(str_row, context)
