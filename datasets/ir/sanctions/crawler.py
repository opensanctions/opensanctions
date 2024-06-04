from urllib.parse import urlencode
from normality import collapse_spaces, slugify
from typing import Dict, Generator, cast
from lxml.etree import _Element
from lxml.html import HtmlElement

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


def parse_table(context: Context, table: HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content()))
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        if len(headers) != len(cells):
            context.log.info("Skipping row with misaligned cells", row=row)
            continue
        yield {hdr: c for hdr, c in zip(headers, cells)}


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
        rows = parse_table(context, doc.find(".//table"))
        if not rows:
            break

        for row in rows:
            crawl_item(context, row)

        page += 1
        assert page < 20, page
