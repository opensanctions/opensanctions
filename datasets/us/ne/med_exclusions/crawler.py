from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

PAGE_SETTINGS = {"join_y_tolerance": 2}


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context, "source.pdf", context.data_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        headers_per_page=True,
        page_settings=lambda page: (page, PAGE_SETTINGS),
    ):
        context.log.info(item)
        crawl_item(item, context)
