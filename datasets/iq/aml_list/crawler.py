from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h


YEARS_LISTS = [
    "2017",
    # "2018",
    # "2019",
    # "2020",
    # "2021",
    # "2022",
    # "2023",
    # "2024",
    # "2025",
]


def crawl_item(item: dict, context: Context):
    pass


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath('//article[@id="post-2171"]//a/@href')
    assert len(url) == 1, url
    url = url[0]
    assert url.endswith(".xlsx"), url
    assert "القوائم-المحلية" in url, url

    path = context.fetch_resource("source.xlsx", url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    for year in YEARS_LISTS:
        for item in h.parse_xlsx_sheet(context, wb[year], skiprows=2):
            crawl_item(item, context)
