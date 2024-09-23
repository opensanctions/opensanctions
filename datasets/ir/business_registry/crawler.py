from urllib.parse import urljoin

from zavod import Context, helpers as h


URL = "https://www.unitedagainstnucleariran.com/iran-business-registry?page={}"


def crawl(context: Context):
    page_number = 0
    pages_processed = 0

    while True:
        # Construct the URL for the current page
        url = URL.format(page_number)
        context.log.info(f"Fetching URL: {url}")

        doc = context.fetch_html(url)
        table = doc.find(".//div[@class='view-content']//table")

        if table is None:
            print("No more tables found.")
            break

        for row in table.findall(".//tr"):
            # Extract the company name and link
            company_elem = row.find(".//td[@headers='view-title-table-column']//a")
            company_name = (
                company_elem.text_content().strip()
                if company_elem is not None
                else None
            )
            company_link = (
                urljoin(url, company_elem.get("href", "").strip())
                if company_elem is not None
                else None
            )

            # Extract withdrawn status
            withdrawn_status_elem = row.xpath(
                ".//td[@headers='view-status-of-business-table-column']//div[@class='featured']"
            )
            is_withdrawn = bool(withdrawn_status_elem)

            # Instead of passing raw HtmlElement row, extract the data
            extracted_data = {
                "company_name": company_name,
                "company_link": company_link,
                "is_withdrawn": is_withdrawn,
            }

            # Process the row data
            for row in h.parse_html_table(table):
                str_row = h.cells_to_str(row)
                nationality = str_row.pop("nationality")
                entity = context.make("LegalEntity")
                entity.id = context.make_id(company_name, nationality)
                entity.add("name", company_name)
                entity.add("country", nationality)
                entity.add("sourceUrl", company_link)
                entity.add("description", str_row.pop("stock_symbol"))
                if is_withdrawn is False:
                    entity.add("topics", "export.risk")

                context.emit(entity, target=True)

            # Now audit with cleaned-up data
            context.audit_data(extracted_data)

        page_number += 1
        pages_processed += 1

        # Limit to 4 pages
        assert pages_processed <= 4, "More pages than expected."
