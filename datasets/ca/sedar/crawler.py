# from rigour.mime.types import CSV
from lxml import etree

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

# # url = "https://sedarplus.ca/c99a4269-161c-4242-a3f0-28d44fa6ce24"
# url = "https://www.sedarplus.ca/csa-order/viewInstance/resource.html?node=W129&id=5b948c1fc5c6f840eaad195c22a2fea6f21b83c12010bb52"
headers = {
    "Accept": "*/*",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Referer": "https://www.sedarplus.ca/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Origin": "https://www.sedarplus.ca",
}
data = {
    "cid": "brc9",
    "uzl": "2N5I9AZPwrU1tpSaFJkb89dyE8oDchr4+rEaJHakPTw=",
    "et": "85",
    "url": "https://www.sedarplus.ca/click/csa-order/viewInstance/view.html?id=5b948c1fc5c6f840eaad195c22a2fea6f21b83c12010bb52&_timestamp=154391777938717",
}


# def crawl(context: Context):
#     path = context.fetch_resource(
#         "source.csv",
#         url,
#         headers=headers,
#         data=data,
#         method="POST",
#     )
#     context.export_resource(path, CSV, title=context.SOURCE_TITLE)


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//table[@aria-label='List of data items']") is not None


def crawl(context: Context):
    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator,
        cache_days=3,
        # headers=headers,
        # data=data,
        # method="POST",
    )
    print(doc.text)
    table = doc.xpath('.//table[@aria-label="List of data items"]')
    print(table)

    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        print(str_row)
