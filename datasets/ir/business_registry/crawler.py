from lxml import html
from rigour.mime.types import HTML
from zavod import Context, helpers as h


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        doc = html.parse(fh)

    # Locate the specific table in the HTML document
    table = doc.find(".//div[@class='view-content']//table")

    # Iterate over rows in the table parsing necessary data
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)

        entity_name = str_row.pop("company_sort_descending")
        nationality = str_row.pop("nationality")
        entity = context.make("LegalEntity")

        entity.id = context.make_id(entity_name, nationality)
        entity.add("name", entity_name)
        entity.add("country", nationality)
        entity.add("description", str_row.pop("stock_symbol", ""))

        context.emit(entity, target=True)
        context.audit_data(str_row, ignore=["withdrawn"])
