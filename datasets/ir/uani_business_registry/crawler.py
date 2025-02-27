from typing import Dict, List
from lxml import etree
from lxml.html import HtmlElement
from normality import slugify

from zavod import Context, Entity, helpers as h
from zavod.shed.zyte_api import fetch_html


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


def is_500_page(doc: etree._Element) -> bool:
    return (
        "The website encountered an unexpected error. Try again later."
        in doc.text_content()
    )


def crawl_subpage(context: Context, url: str, entity: Entity, entity_id):
    context.log.debug(f"Starting to crawl company page: {url}")
    validator_xpath = (
        './/div[@class="c-full-node__info"] | '
        './/*[contains(text(), "The website encountered an unexpected error.")]'
    )
    doc = fetch_html(context, url, validator_xpath, cache_days=3)
    if is_500_page(doc):
        context.log.info(f"Broken link detected: {url}")
        return
    doc.make_links_absolute(url)

    facts_lists = doc.xpath('.//div[@class="c-full-node__info"]')
    assert len(facts_lists) == 1
    facts = parse_facts_list(facts_lists[0])

    for industry in facts.pop("industry", []):
        entity.add("sector", industry.text_content().strip())

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

    for website in facts.pop("website", []):
        entity.add("website", website.xpath(".//a/@href"))

    for owner in facts.pop("parent_company", []):
        parent_company = owner.text_content().strip()
        parent_urls = owner.xpath(".//a/@href")
        parent = context.make("Company")
        parent.id = context.make_id(parent_company, *parent_urls, prefix="ir-br-co")
        parent.add("name", parent_company)
        parent.add("sourceUrl", parent_urls)
        context.emit(parent)
        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity_id, parent.id, prefix="ir-br-own")
        ownership.add("asset", entity.id)
        ownership.add("owner", parent.id)
        context.emit(ownership)

    # Most of the time this is the subsidiary, but JX Nippon Oil & Energy
    # and Japan Energy Corporation are only affiliates
    for affiliate in facts.pop("affiliates_subsidiaries", []):
        affiliate_name = affiliate.text_content().strip()
        affiliates_urls = affiliate.xpath(".//a/@href")
        subsidiary = context.make("Company")
        subsidiary.id = context.make_id(
            affiliate_name, *affiliates_urls, prefix="ir-br-co"
        )
        subsidiary.add("name", affiliate_name)
        subsidiary.add("sourceUrl", affiliates_urls)
        context.emit(subsidiary)

        link = context.make("UnknownLink")
        left = min(entity_id, subsidiary.id)
        right = max(entity_id, subsidiary.id)
        link.id = context.make_id(left, right, prefix="ir-br-link")
        link.add("subject", left)
        link.add("object", right)
        link.add("role", "affiliate")
        context.emit(link)

    context.audit_data(
        facts,
        ignore=[
            "country",
            "symbol",
            "contact_information",
            "response",
            "value_of_usg",
        ],
    )


def crawl(context: Context):
    pages_processed = 0

    while True:
        # Construct the URL for the current page
        url = f"https://www.unitedagainstnucleariran.com/iran-business-registry?page={pages_processed}"
        context.log.info(f"Fetching URL: {url}")

        # Fetch the HTML and get the table
        doc = fetch_html(context, url, ".//div[@class='o-grid']", cache_days=3)
        doc.make_links_absolute(url)
        table = doc.find(".//div[@class='view-content']//table")
        if table is None:
            context.log.info("No more tables found.")
            break

        # Iterate through the parsed table
        for row in h.parse_html_table(table, skiprows=1):
            str_row = h.cells_to_str(row)

            withdrawn_elem = row.pop("withdrawn")
            is_withdrawn = bool(withdrawn_elem.xpath('.//div[@class="featured"]'))
            if is_withdrawn is True:
                continue

            company_elem = row.pop("company_sort_descending")
            company_link = company_elem.find(".//a").get("href", "").strip()
            company_name = str_row.pop("company_sort_descending")

            # Create and emit an entity
            entity = context.make("Company")
            entity.id = context.make_id(company_name, company_link, prefix="ir-br-co")

            crawl_subpage(context, company_link, entity, entity.id)
            entity.add("name", company_name)
            entity.add("country", str_row.pop("nationality"))
            entity.add("sourceUrl", company_link)
            entity.add("ticker", str_row.pop("stock_symbol"))
            entity.add("topics", "export.risk")
            context.emit(entity)
            context.audit_data(str_row)

        pages_processed += 1

        # Limit the number of pages processed to avoid infinite loops
        assert pages_processed <= 10, "More pages than expected."
