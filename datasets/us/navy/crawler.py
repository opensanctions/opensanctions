from lxml import html
from zavod import Context, helpers as h
from typing import Dict, Generator


def crawl_item(context: Context, item: html.HtmlElement):
    # Extract name and URL
    name_el = item.find(".//a")
    name = name_el.text_content().strip()
    url = name_el.get("href")

    # Create entity and set properties
    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("url", url)
    entity.add("role", "Senior Executive")

    # Emit the entity
    context.emit(entity)


def parse_items(doc: html.HtmlElement) -> Generator[html.HtmlElement, None, None]:
    # Locate each biography link item
    for item in doc.findall('.//li[@class="dfwp-item"]'):
        yield item


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for item in parse_items(doc):
        crawl_item(context, item)
