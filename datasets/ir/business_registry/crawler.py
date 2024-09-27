from typing import Dict, List
from lxml import etree
from lxml.html import HtmlElement
from urllib.parse import urljoin

from normality import slugify

from zavod import Context, Entity, helpers as h
from zavod.shed.zyte_api import fetch_html


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//div[@class='o-grid']") is not None


def extract_text(doc, xpath_query):
    result = doc.xpath(xpath_query)
    assert len(result) == 1, (xpath_query, result)
    return result[0].strip() if result else None


def check_url_status(context: Context, url: str) -> bool:
    try:
        response = context.fetch_html(url, cache_days=1)
        return response is not None
    except Exception as e:
        context.log.warning(f"Broken link detected: {url} - {e}")
        return False


def parse_facts_list(container: HtmlElement) -> Dict[str, List[HtmlElement]]:
    """
    Parse a list of facts into a dictionary.

    Args:
        container: The HtmlElement containing the description list rows

    Returns:
        Dictionary where the slugified text content of each data list label is the key,
          and the value element is the value.
    """
    rows_xpath = './/div[contains(@class, "c-full-node__info--row")]'
    key_xpath = './/label[contains(@class, "field-label-inline")]'
    values_xpath = "./span"
    data = {}
    for row in container.xpath(rows_xpath):
        key_els = row.xpath(key_xpath)
        assert len(key_els) == 1, (key_xpath, row.text_content())
        label = key_els[0].text_content()
        key = slugify(label, sep="_")
        assert bool(key), (label, key)
        assert key not in data, (key, data)
        value_els = row.xpath(values_xpath)
        data[key] = value_els
    return data


def crawl_subpage(context: Context, url: str, entity: Entity, entity_id):
    if not check_url_status(context, url):
        return None, None, None, None, None, None

    context.log.debug(f"Starting to crawl personal page: {url}")
    doc = context.fetch_html(url, cache_days=3)
    facts_lists = doc.xpath('.//div[@class="c-full-node__info"]')
    assert len(facts_lists) == 1
    facts = parse_facts_list(facts_lists[0])

    for sources in facts.pop("sources", []):
        for source in sources.xpath(".//p"):
            source_text = source.text_content()
            if "initWindowFocus" in source_text:
                continue
            for a in source.xpath(".//a"):
                source_url = a.get("href")
                if source_url:
                    source_text += f" ({source_url})"
            entity.add("notes", source_text)

        # entity.add("website", website)
        # entity.add("sector", industry)
    #
    # if parent_company is not None:
    #    parent = context.make("Company")
    #    parent.id = context.make_id(parent_company, parent_url)
    #    parent.add("name", parent_company)
    #    parent.add("sourceUrl", parent_url)
    #    context.emit(parent)
    #
    #    own1 = context.make("Ownership")
    #    own1.id = context.make_id(entity_id, parent.id)
    #    own1.add("asset", entity.id)
    #    own1.add("owner", parent.id)
    #    context.emit(own1)
    #    entity.add("parent", parent.id)
    # if affiliates is not None:
    #    subsidiary = context.make("Company")
    #    subsidiary.id = context.make_id(affiliates, affiliates_url)
    #    subsidiary.add("name", affiliates)
    #    subsidiary.add("sourceUrl", affiliates_url)
    #    context.emit(subsidiary)
    #    # entity.add("subsidiaries", subsidiary.id)
    #
    #    own2 = context.make("Ownership")
    #    own2.id = context.make_id(entity_id, subsidiary.id)
    #    own2.add("asset", subsidiary.id)
    #    own2.add("owner", entity.id)
    #    context.emit(own2)

    context.audit_data(facts)
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
