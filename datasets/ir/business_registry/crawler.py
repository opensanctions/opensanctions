from urllib.parse import urljoin
from lxml import etree

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//div[@class='view-content']//table") is not None


def crawl(context: Context):
    page_number = 0
    pages_processed = 0

    while True:
        # Construct the URL for the current page
        url = f"https://www.unitedagainstnucleariran.com/iran-business-registry?page={page_number}"
        context.log.info(f"Fetching URL: {url}")

        # Fetch the HTML and get the table
        doc = fetch_html(context, url, unblock_validator, cache_days=3)
        table = doc.find(".//div[@class='view-content']//table")
        if table is None:
            context.log.info("No more tables found.")
            break

        # Iterate through the parsed table
        for row in h.parse_html_table(table):
            str_row = h.cells_to_str(row)

            company_elem = row.pop("company_sort_descending")
            # Capture the link from the anchor tag
            company_link = urljoin(
                url, company_elem.find(".//a").get("href", "").strip()
            )
            withdrawn_elem = row.pop("withdrawn")
            is_withdrawn = bool(withdrawn_elem)

            company_name = str_row.pop("company_sort_descending")
            nationality = str_row.pop("nationality")
            stock_symbol = str_row.pop("stock_symbol")

            # Create and emit an entity
            entity = context.make("Company")
            entity.id = context.make_id(company_name, nationality)
            entity.add("name", company_name)
            entity.add("country", nationality)
            entity.add("sourceUrl", company_link)
            entity.add("description", stock_symbol)
            if is_withdrawn is False:
                entity.add("topics", "export.risk")

            context.emit(entity, target=True)
            context.audit_data(str_row)

        page_number += 1
        pages_processed += 1

        # Limit the number of pages processed to avoid infinite loops
        assert pages_processed <= 4, "More pages than expected."
