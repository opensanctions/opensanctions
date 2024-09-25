from lxml import etree
from urllib.parse import urljoin
from typing import List, Optional, Tuple

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//div[@class='o-grid']") is not None


def extract_text(doc, xpath_query):
    result = doc.xpath(xpath_query)
    return result[0].strip() if result else None


def check_url_status(context: Context, url: str) -> bool:
    try:
        response = context.fetch_html(url, cache_days=1)
        return response is not None
    except Exception as e:
        context.log.warning(f"Broken link detected: {url} - {e}")
        return False


def crawl_subpage(
    context: Context, url: str
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[List[str]],
    Optional[str],
    Optional[str],
    Optional[List[str]],
]:
    if not check_url_status(context, url):
        return None, None, None, None, None, None

    context.log.debug(f"Starting to crawl personal page: {url}")
    doc = context.fetch_html(url, cache_days=3)

    (
        industry,
        website,
        parent_company,
        affiliates_subsidiaries,
        sources_text,
    ) = (None, None, None, None, [])
    emails: List[str] = []

    info_rows = doc.xpath('.//div[@class="c-full-node__info--row"]')

    for row in info_rows:
        label = extract_text(row, './label[@class="field-label-inline"]/text()')

        if label == "Industry:":
            industry = extract_text(row, "./span/text()")
        elif label.lower() == "website:":
            website = extract_text(row, "./span/a/@href")
        elif label == "Contact Information:":
            emails = row.xpath('./span/p/a[starts-with(@href, "mailto:")]/text()')
        elif label == "Parent Company:":
            parent_company = extract_text(row, "./span/a/text()")
        elif label == "Affiliates/Subsidiaries:":
            affiliates_subsidiaries = extract_text(row, "./span/a/text()")
        elif label == "Sources:":
            sources = row.xpath(".//span//p")
            sources_text = [src.xpath("string()").strip() for src in sources]

    return (
        industry,
        website,
        emails,
        parent_company,
        affiliates_subsidiaries,
        sources_text,
    )


def crawl(context: Context):
    pages_processed = 0

    while True:
        # Construct the URL for the current page
        url = f"https://www.unitedagainstnucleariran.com/iran-business-registry?page={pages_processed}"
        context.log.info(f"Fetching URL: {url}")

        # Fetch the HTML and get the table
        doc = fetch_html(context, url, unblock_validator, cache_days=3)
        table = doc.find(".//div[@class='view-content']//table")
        if table is None:
            context.log.info("No more tables found.")
            break

        # Iterate through the parsed table
        for row in h.parse_html_table(table, skiprows=1):
            str_row = h.cells_to_str(row)

            company_elem = row.pop("company_sort_descending")
            company_link = urljoin(
                url, company_elem.find(".//a").get("href", "").strip()
            )
            withdrawn_elem = row.pop("withdrawn")
            is_withdrawn = bool(withdrawn_elem.xpath('.//div[@class="featured"]'))

            company_name = str_row.pop("company_sort_descending")
            nationality = str_row.pop("nationality")
            stock_symbol = str_row.pop("stock_symbol")

            (
                industry,
                website,
                emails,
                parent_company,
                affiliates_subsidiaries,
                sources_text,
            ) = crawl_subpage(context, company_link)

            # Create and emit an entity
            entity = context.make("Company")
            entity.id = context.make_id(company_name, nationality)
            entity.add("name", company_name)
            entity.add("country", nationality)
            entity.add("sourceUrl", company_link)
            entity.add("ticker", stock_symbol)
            entity.add("sector", industry)
            entity.add("website", website)
            if emails is None:
                continue
            for email in emails:
                entity.add("email", email)
            if parent_company is not None:
                parent_com = context.make("Company")
                parent_com.id = context.make_id(parent_company)
                parent_com.add("name", parent_company)
                entity.add("parent", parent_com)
            if affiliates_subsidiaries is not None:
                subsidiary = context.make("Company")
                subsidiary.id = context.make_id(affiliates_subsidiaries)
                subsidiary.add("name", affiliates_subsidiaries)
                entity.add("subsidiaries", affiliates_subsidiaries)
            entity.add("notes", sources_text)
            if is_withdrawn is True:
                entity.add("topics", "export.control")
                target = False
            else:
                entity.add("topics", "export.risk")
                target = True

            context.emit(entity, target=target)
            context.audit_data(str_row)

        pages_processed += 1

        # Limit the number of pages processed to avoid infinite loops
        assert pages_processed <= 10, "More pages than expected."
