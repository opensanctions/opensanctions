from typing import Dict, Generator, List, Tuple
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context
from zavod import helpers as h


def parse_table(table) -> Generator[Dict[str, str | Tuple[str]], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                headers.append(slugify(el.text_content()))
            continue

        cells = []
        for el in row.findall("./td"):
            a = el.find(".//a")
            if a is None:
                cells.append(collapse_spaces(el.text_content()))
            else:
                cells.append((collapse_spaces(a.text_content()), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_entity(context: Context, url: str, name: str, category: str) -> None:
    res = context.lookup("schema", category)
    entity = context.make(res.schema)
    entity.id = context.make_id(name)
    entity.add("name", name)

    sanction = h.make_sanction(context, entity, key=category)
    sanction.add(
        "program",
        f"Public Alerts: Unregistered Soliciting Entities (PAUSE)",
    )
    if category != "":
        sanction.add("reason", category)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)

    table = doc.find(".//table")
    for row in parse_table(table):
        name, url = row.pop("name")
        name = name.replace("Name: ", "")
        category = row.pop("category").replace("Category:", "").strip()
        crawl_entity(context, url, name, category)
        context.audit_data(row)
