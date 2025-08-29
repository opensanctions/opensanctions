from lxml import html
from lxml.etree import _Element
from normality import slugify
from banal import first
from urllib.parse import urljoin
from rigour.mime.types import HTML
from typing import List, Optional

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ID_SPLITS = ["(a) ", "(b) ", "(c) ", "(d) ", "(e) ", "(f) "]


def cell_values(el: _Element) -> List[str]:
    items = [i.text_content().strip() for i in el.findall(".//li")]
    if len(items):
        return items
    text = el.text_content().strip()
    if text == "-":
        return []
    return [text]


def crawl_table(context: Context, table: _Element) -> None:
    headers: Optional[List[str]] = None
    for row in table.findall(".//tr"):
        cells = row.findall(".//td")
        if headers is None:
            texts = [c.text_content().strip() for c in cells]
            headers = [slugify(t.split(")")[1], sep="_") for t in texts]
            headers = [context.lookup_value("columns", hd, hd) for hd in headers]
            continue
        values = [cell_values(c) for c in cells]
        row = dict(zip(headers, values))
        if "date_of_birth" in headers:
            schema = "Person"
            key = "person"
        else:
            schema = "Organization"
            key = "group"
        entity = context.make(schema)
        reference = first(row.pop("reference"))
        entity.id = context.make_slug(key, reference)
        for name in row.pop("name"):
            entity.add("name", name.split("@"))
        entity.add("topics", "sanction")
        entity.add("alias", row.pop("alias", []))
        entity.add("alias", row.pop("other_name", []))
        entity.add("address", row.pop("address"))

        if entity.schema.is_a("Person"):
            entity.add("title", row.pop("title", None))
            h.apply_dates(entity, "birthDate", row.pop("date_of_birth", None))
            entity.add("birthPlace", row.pop("place_of_birth", None))
            entity.add("nationality", row.pop("nationality", None))
            entity.add("passportNumber", row.pop("passport_number", None))
            for id in h.multi_split(row.pop("id_number", None), ID_SPLITS):
                entity.add("idNumber", id)

        sanction = h.make_sanction(context, entity)
        h.apply_dates(sanction, "listingDate", row.pop("date_of_listed"))
        sanction.add("authorityId", reference)
        sanction.add("program", row.pop("designation", None))

        context.emit(entity)
        context.emit(sanction)
        context.audit_data(row, ignore=["no"])


def crawl_html_url(context: Context) -> str:
    validator = ".//*[contains(text(), 'LIST OF SANCTIONS UNDER THE MINISTRY OF HOME AFFAIRS (MOHA)')]"
    html = fetch_html(context, context.data_url, validator, cache_days=5)
    for a in html.findall('.//div[@class="uk-container"]//a'):
        if "sanctions list" not in a.text_content().lower():
            continue
        if ".html" in a.get("href", ""):
            return urljoin(context.data_url, a.get("href"))
    raise ValueError("No HTML found")


def crawl(context: Context):
    html_url = crawl_html_url(context)
    path = context.fetch_resource("source.html", html_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)

    with open(path, "rb") as fh:
        doc = html.fromstring(fh.read())
        for table in doc.findall(".//table"):
            crawl_table(context, table)
