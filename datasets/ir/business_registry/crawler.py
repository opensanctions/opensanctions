from lxml import etree
from lxml.html import HtmlElement
from normality import slugify
from typing import Generator, Dict
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def parse_html_table(
    context: Context,
    table: HtmlElement,
    header_tag: str = "th",
    skiprows: int = 0,
) -> Generator[Dict[str, str | None], None, None]:
    headers = None
    row_counter = 0
    for row in table.findall(".//tr"):
        row_counter += 1
        if row_counter <= skiprows:
            continue

        if headers is None:
            header_elements = row.findall(f"./{header_tag}")
            if not header_elements:
                continue

            headers = [
                slugify(el.text_content().strip(), sep="_") or f"column_{i}"
                for i, el in enumerate(header_elements)
            ]
            continue

        cells = row.findall("./td")
        if len(cells) != len(headers):
            context.log.warning(
                f"Skipping row {row_counter} due to mismatch in number of cells and headers."
            )
            continue
        yield {hdr: c for hdr, c in zip(headers, cells)}


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//div[@class='o-grid']") is not None


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
        for row in parse_html_table(context, table, skiprows=1):
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
            else:
                entity.add("topics", "export.control")

            context.emit(entity)
            context.audit_data(str_row)

        page_number += 1
        pages_processed += 1

        # Limit the number of pages processed to avoid infinite loops
        assert pages_processed <= 4, "More pages than expected."
