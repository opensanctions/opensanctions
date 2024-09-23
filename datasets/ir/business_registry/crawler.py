from lxml import html
from rigour.mime.types import HTML
from zavod import Context, helpers as h

URL = "https://www.unitedagainstnucleariran.com/iran-business-registry?page={}"


def crawl(context: Context):
    page_number = 0
    pages_processed = 0
    max_pages = 5

    while True:
        # Construct the URL for the current page
        url = URL.format(page_number)
        print(f"Fetching URL: {url}")  # Debug line to show progress

        # Fetch and parse the HTML document
        doc = context.fetch_html(url)

        # Locate the specific table in the HTML document
        table = doc.find(".//div[@class='view-content']//table")

        if table is None:
            print("No more tables found, stopping crawl.")
            break

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

        page_number += 1
        pages_processed += 1

        # Check if we should stop crawling
        if pages_processed >= max_pages:
            print("Reached the maximum number of pages to process.")
            break
