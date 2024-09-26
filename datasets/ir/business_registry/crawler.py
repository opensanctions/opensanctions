from lxml import etree
from urllib.parse import urljoin
from typing import List

from zavod import Context, Entity, helpers as h
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


def crawl_subpage(context: Context, url: str, entity: Entity, entity_id):
    if not check_url_status(context, url):
        return None, None, None, None, None, None

    context.log.debug(f"Starting to crawl personal page: {url}")
    doc = context.fetch_html(url, cache_days=3)
    (
        industry,
        website,
        parent_company,
        parent_url,
        affiliates,
        affiliates_url,
        sources_text,
    ) = (None, None, None, None, None, None, None)

    info_rows = doc.xpath('.//div[contains(@class, "c-full-node__info--row")]')

    for row in info_rows:
        label = extract_text(row, './label[@class="field-label-inline"]/text()')

        if "field_industry" in row.attrib.get("class", ""):
            industry = extract_text(row, "./span/text()")
            print("Industry:", industry)  # Debug print

        elif "field_source" in row.attrib.get("class", ""):
            sources = row.xpath(".//span//p")
            sources_text = [src.xpath("string()").strip() for src in sources]
            print("Sources:", sources_text)  # Debug print

        elif label == "website:":
            website = extract_text(row, "./span/a/@href")
            print("Website:", website)  # Debug print

        elif label == "Parent Company:":
            parent_company = extract_text(row, "./span/a/text()")
            parent_url = extract_text(row, "./span/a/@href")
            if parent_url:
                parent_url = urljoin(url, parent_url)

        elif label == "Affiliates/Subsidiaries:":
            affiliates = extract_text(row, "./span/a/text()")
            affiliates_url = extract_text(row, "./span/a/@href")
            if affiliates_url:
                affiliates_url = urljoin(url, affiliates_url)

        entity.add("notes", sources_text)
        entity.add("website", website)
        entity.add("sector", industry)

        if parent_company is not None:
            parent = context.make("Company")
            parent.id = context.make_id(parent_company, parent_url)
            parent.add("name", parent_company)
            parent.add("sourceUrl", parent_url)
            context.emit(parent)

            own1 = context.make("Ownership")
            own1.id = context.make_id(entity_id, parent.id)
            own1.add("asset", entity.id)
            own1.add("owner", parent.id)
            context.emit(own1)
            entity.add("parent", parent.id)
        if affiliates is not None:
            subsidiary = context.make("Company")
            subsidiary.id = context.make_id(affiliates, affiliates_url)
            subsidiary.add("name", affiliates)
            subsidiary.add("sourceUrl", affiliates_url)
            context.emit(subsidiary)
            # entity.add("subsidiaries", subsidiary.id)

            own2 = context.make("Ownership")
            own2.id = context.make_id(entity_id, subsidiary.id)
            own2.add("asset", subsidiary.id)
            own2.add("owner", entity.id)
            context.emit(own2)

    return None


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

            # Create and emit an entity
            entity = context.make("Company")
            entity.id = context.make_id(company_name, nationality)
            entity_id = entity.id

            crawl_subpage(context, company_link, entity, entity_id)
            entity.add("name", company_name)
            entity.add("country", nationality)
            entity.add("sourceUrl", company_link)
            entity.add("ticker", stock_symbol)

            if is_withdrawn is True:
                target = False
            else:
                entity.add("topics", "export.risk")
                target = True

            context.emit(entity, target=target)
            context.audit_data(str_row)

        pages_processed += 1

        # Limit the number of pages processed to avoid infinite loops
        assert pages_processed <= 10, "More pages than expected."
