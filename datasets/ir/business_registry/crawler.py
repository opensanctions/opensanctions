from lxml import etree
from urllib.parse import urljoin

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
        context.log.info(f"Broken link {url}: {e}")
        return False


def crawl_subpage(context: Context, url: str):
    if not check_url_status(context, url):
        context.log.warning(f"Broken link detected: {url}")
        return None

    context.log.debug(f"Starting to crawl personal page: {url}")
    doc = context.fetch_html(url, cache_days=1)

    # Extract additional details from the personal page
    industry = extract_text(
        doc, './/div[@class="c-full-node__info--row field_industry"]/span/text()'
    )
    website = extract_text(doc, './/div[@class="c-full-node__info--row"]/span/a/@href')
    email = extract_text(
        doc,
        './/div[@class="c-full-node__info--row"]/span/p/a[starts-with(@href, "mailto:")]/text()',
    )

    # Extract sources
    sources = doc.xpath('.//div[@class="c-full-node__info--row field_source"]//p')
    sources_text = [src.xpath("string()").strip() for src in sources]

    return industry, website, email, sources_text


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

            industry, website, email, sources_text = crawl_subpage(
                context, company_link
            )

            # Create and emit an entity
            entity = context.make("Company")
            entity.id = context.make_id(company_name, nationality)
            entity.add("name", company_name)
            entity.add("country", nationality)
            entity.add("sourceUrl", company_link)
            entity.add("description", stock_symbol)
            entity.add("sector", industry)
            entity.add("website", website)
            entity.add("email", email)
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
