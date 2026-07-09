from zavod import Context, helpers as h
from zavod.util import Element

# SEC's Fair Access policy blocks browser-mimicking user agents and requires
# a declared UA with contact info. See https://www.sec.gov/developer
HEADERS = {"User-Agent": "OpenSanctions tech@opensanctions.org"}


def crawl_item(item: Element, context: Context) -> None:
    names = h.split_comma_names(context, h.element_text(item))

    source_url = item.get("href")

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("topics", "crime.fin")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity)

        sanction.add("sourceUrl", source_url)

        context.emit(entity)
        context.emit(sanction)


def crawl(context: Context) -> None:
    response = context.fetch_html(
        context.data_url, headers=HEADERS, absolute_links=True
    )
    for item in h.xpath_elements(
        response,
        './/*[contains(text(), "Search Cases:")]/../following-sibling::ul/li/a',
    ):
        crawl_item(item, context)
