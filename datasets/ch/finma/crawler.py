from typing import Any, Dict
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h


def crawl(context: Context, data: Dict[str, Any]):
    res = context.http.post(context.data_url, data=data)
    for item in res.json()["Items"]:
        url = urljoin(context.data_url, item.pop("Link"))
        entity = context.make("LegalEntity")
        name = item.pop("Title")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("country", "ch")
        entity.add("topics", "crime.fin")
        entity.add("sourceUrl", url)
        # entity.add("notes", item.pop("FacetColumn"))
        h.apply_date(entity, "createdAt", item.pop("Date"))

        try:
            html = context.fetch_html(url, cache_days=30)
        except Exception as exc:
            if "404" in str(exc):
                continue
            context.log.warn("Cannot fetch item: %s" % url)
            continue
        for row in html.findall('.//div[@class="l-main"]//table//tr'):
            header = row.findtext("./th")
            cell = row.find("./td")
            if cell is None or header is None:
                continue
            value = str(cell.xpath("string()"))
            if len(value.strip()) < 2:
                continue
            if header == "Name":
                entity.add("name", value)
            elif header in ("Address", "Domicile"):
                entity.add("address", value)
            elif header in ("Ruling dated"):
                h.apply_date(entity, "modifiedAt", value)
            elif header in ("Remarks", "Details"):
                entity.add("description", value)
            elif header == "Internet":
                entity.add("website", value)
            elif header in ("Commercial register"):
                continue
            else:
                context.log.warn("Unknown header: %s" % header, value=value, url=url)

        context.emit(entity)


def crawl_warnings(context: Context):
    data = {"ds": "{1C6B8731-638C-4003-A93C-A625BF7A6800}", "Order": "1"}
    crawl(context, data)


def crawl_rulings(context: Context):
    data = {"ds": "{20B37C3A-01FC-42B8-A031-847154F8BBDF}", "Order": "4"}
    crawl(context, data)
