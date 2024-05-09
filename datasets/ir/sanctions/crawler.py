from urllib.parse import urlencode
from normality import collapse_spaces
from typing import Dict
from lxml.etree import _Element

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(context: Context, row: Dict[str, str]):
    entity_type = row.pop("sanction-type")
    schema = context.lookup_value("schema", entity_type)
    if schema is None:
        context.log.warning(f"Unknown schema: {entity_type}")
        return

    name = row.pop("name")
    position = row.pop("position").strip()

    entity = context.make(schema)
    entity.id = context.make_id(name, position)

    entity.add("name", name)
    entity.add("country", row.pop("country") or None)
    entity.add("topics", "sanction.counter")
    if schema == "Person" and position.replace("-", ""):
        entity.add("position", position)

    sanction = h.make_sanction(context, entity)
    sanction_date = collapse_spaces(row.pop("sanction-date"))
    sanction_dates = h.parse_date(sanction_date, formats=["%B %d %Y"])
    sanction.add("date", sanction_dates)
    sanction.add("description", row.pop("sanction-title"))

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(row, ignore=[None])


def unblock_validator(el: _Element) -> bool:
    return "Lookup Results" in el.text_content()


def crawl(context: Context):
    page = 1
    max_page = None

    while max_page is None or page <= max_page:
        params = {"page": page}
        qs = urlencode(params)
        url = f"{context.data_url}?{qs}"
        doc = fetch_html(context, url, unblock_validator, cache_days=7)
        pagenums = [
            int(el.text_content())
            for el in doc.xpath('//*[contains(@class, "pageinationnum")]')
        ]
        max_page = max(pagenums)
        rows = h.parse_table(doc.find(".//table"))
        if not rows:
            break

        for row in rows:
            crawl_item(context, row)

        page += 1
        assert page < 20, page
