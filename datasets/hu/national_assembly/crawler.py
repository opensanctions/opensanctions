from lxml import html
from zavod import Context

# from zavod import helpers as h

from zavod.shed import zyte_api


def crawl_row(context: Context, row: html.Element) -> None:
    url_el = row.find(".//a[@href]")
    if url_el is None:
        context.log.warning("No URL found in row, skipping")
        return
    url = url_el.get("href")
    assert url is not None, "No URL found in row"
    unblock_pep = ".//div[@class='pair-content']"
    pep_doc = zyte_api.fetch_html(context, url, unblock_pep, cache_days=1)

    # print(html.tostring(pep_doc, pretty_print=True, encoding="unicode"))
    pep_name = pep_doc.xpath(".//div[@class='pair-content']/text()")
    print(pep_name[0])
    # name = h.element_text(pep_name)


def crawl(context: Context):
    unblock = ".//table[@class=' table table-bordered']"
    doc = zyte_api.fetch_html(context, context.data_url, unblock, cache_days=1)
    # print(html.tostring(doc, pretty_print=True, encoding="unicode"))
    table = doc.find(unblock)
    assert table is not None, "Could not find the main table in the document"
    rows = table.findall(".//tbody/tr")
    for row in rows:
        crawl_row(context, row)
