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

        # Iterate over rows in the table
        rows = table.findall(".//tr")
        for row in rows:
            # Extract the company name and link
            company_elem = row.find(".//td[@headers='view-title-table-column']//a")
            if company_elem is not None:
                company_name = company_elem.text_content().strip()
                company_link = urljoin(url, company_elem.get("href", "").strip())
            else:
                company_name = None
                company_link = None

            # Extract the nationality
            nationality_elem = row.find(
                ".//td[@headers='view-field-country-table-column']"
            )
            nationality = (
                nationality_elem.text_content().strip()
                if nationality_elem is not None
                else None
            )

            # Extract the stock symbol
            stock_symbol_elem = row.find(
                ".//td[@headers='view-field-symbol-table-column']"
            )
            stock_symbol = (
                stock_symbol_elem.text_content().strip()
                if stock_symbol_elem is not None
                else None
            )
            withdrawn_status_elem = row.xpath(
                ".//td[@headers='view-status-of-business-table-column']//div[@class='featured']"
            )
            is_withdrawn = bool(withdrawn_status_elem)

            # Only create an entity if we have a company name and nationality
            if company_name and nationality:
                entity = context.make("LegalEntity")
                entity.id = context.make_id(company_name, nationality)
                entity.add("name", company_name)
                entity.add("country", nationality)
                entity.add("sourceUrl", company_link)
                entity.add("description", stock_symbol)
                if is_withdrawn is False:
                    entity.add("topics", "export.risk")

                context.emit(entity, target=True)
                context.audit_data(row)

        page_number += 1
        pages_processed += 1

        assert pages_processed <= 4, "More pages than expected."
