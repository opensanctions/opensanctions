from lxml import html
from zavod import Context, helpers as h
from typing import Generator


def crawl_item(context: Context, item: html.HtmlElement):
    # Extract name and URL
    link_el = item.find(".//a")
    url = link_el.get("href")
    name = link_el.find(".//h2").text_content().strip()
    position = link_el.find(".//h3").text_content().strip()

    # Create entity and set properties
    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("position", position)
    # entity.add("role", "Senior Executive")

    # Emit the entity
    context.emit(entity)


def parse_items(doc: html.HtmlElement) -> Generator[html.HtmlElement, None, None]:
    # Locate the specific section containing the biography listing
    section = doc.find('.//section[@class="biography-listing"]')
    if section is not None:
        # Locate each biography link item within the section
        for item in section.findall(".//li"):
            yield item


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    for item in parse_items(doc):
        crawl_item(context, item)
