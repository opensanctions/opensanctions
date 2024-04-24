import re
from normality import collapse_spaces
import requests

from zavod import Context, helpers as h
from typing import Dict
from lxml.etree import _Element
from lxml import html


def crawl_item(context: Context, row: Dict[str, str]):
    entity_type = row.pop("sanction-type")
    schema = context.lookup_value("schema", entity_type)
    if not schema:
        context.log.warning(f"Unknown schema: {entity_type}")

    name = row.pop("name")
    position = row.pop("position").strip()

    entity = context.make(schema)
    entity.id = context.make_id(name, position)

    entity.add("name", name)
    entity.add("country", row.pop("country") or None)
    entity.add("topics", "poi")
    if schema == "Person" and position.replace("-", ""):
        entity.add("position", position)

    sanction = h.make_sanction(context, entity)
    sanction_date = collapse_spaces(row.pop("sanction-date"))
    sanction_date = h.parse_date(sanction_date, formats=["%B %d %Y"])
    sanction.add("date", sanction_date)
    sanction.add("description", row.pop("sanction-title"))

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(row, ignore=[None])


def crawl(context: Context):
    page = 1
    max_page = None

    while max_page is None or page <= max_page:
        params = {"page": page}
        doc = context.fetch_html(
            context.data_url, params=params, unblock_at_cost=True, cache_days=1
        )
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
