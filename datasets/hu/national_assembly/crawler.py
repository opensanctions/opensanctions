from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    table = doc.find(".//table[@class=' table table-bordered']")
    assert table is not None, "Could not find the main table in the document"
    for row in h.parse_html_table(table, index_empty_headers=True):
        print(h.cells_to_str(row))
