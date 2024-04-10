from lxml.etree import _Element

from zavod import Context, helpers as h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
}


def crawl_item(item: _Element, context: Context):
    names = h.split_comma_names(context, item.text_content())

    source_url = item.get("href")

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("topics", "crime.fin")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity)

        sanction.add("sourceUrl", source_url)

        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    response = context.fetch_html(context.data_url, headers=HEADERS)

    response.make_links_absolute(context.data_url)

    for item in response.xpath(
        './/*[contains(text(), "Search Cases:")]/../following-sibling::ul/li/a'
    ):
        crawl_item(item, context)
