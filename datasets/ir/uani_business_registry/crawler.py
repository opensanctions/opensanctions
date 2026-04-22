from lxml import etree
from lxml.html import HtmlElement
from normality import slugify

from zavod import Context, Entity, helpers as h
from zavod.extract.zyte_api import fetch_html
from zavod.util import Element


def parse_facts_list(container: HtmlElement) -> dict[str, list[HtmlElement]]:
    """
    Parse a list of facts into a dictionary.

    Args:
        container: The HtmlElement containing the description list rows

    Returns:
        Dictionary where the slugified text content of each data list label is the key,
          and the value element is the value.
    """
    rows_xpath = './/div[contains(@class, "c-full-node__info--row")]'
    key_xpath = './/label[contains(@class, "field__label")]'
    values_xpath = "./span"
    data: dict[str, list[HtmlElement]] = {}
    for row in container.xpath(rows_xpath):
        key_els = row.xpath(key_xpath)
        assert len(key_els) == 1, (key_xpath, row.text_content())
        label = key_els[0].text_content()
        key = slugify(label, sep="_")
        assert key
        assert key not in data, (key, data)
        value_els = row.xpath(values_xpath)
        data[key] = value_els
    return data


def crawl_subpage(context: Context, url: str, entity: Entity, entity_id: str) -> None:
    context.log.info(f"Starting to crawl company page: {url}")
    # In the past we've gotten an error message
    # "The website encountered an unexpected error. Try again later."
    # In that case this validator doesn't match.
    # If we get UnblockFailedExceptions again, it could be due to that. If that happens,
    # To confirm that locally, run with --debug.
    # To confirm in prod, one option is to add
    # '| .//*[contains(text(), "The website encountered an unexpected error.")]'
    # to the unblock validator and then invalidate the cache and log the error.
    # BEWARE skipping pages with this error means intermittent data loss
    # and we've had complaints about that on this dataset in the past.
    validator_xpath = './/div[@class="c-full-node__info"]'
    doc = fetch_html(
        context,
        url,
        validator_xpath,
        cache_days=3,
        geolocation="us",
        absolute_links=True,
    )

    facts_list = h.xpath_element(doc, './/div[@class="c-full-node__info"]')
    facts = parse_facts_list(facts_list)

    for industry in facts.pop("industry", []):
        entity.add("sector", industry.text_content().strip())

    for sources in facts.pop("sources", []):
        for source in sources.xpath(".//p"):
            # Sometimes, the tree contains some weird CSS elements
            # with something that looks like an HTML comment - get rid of those.
            etree.strip_elements(source, "style", "script")
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
        assert subsidiary.id
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


def get_end_page(doc: Element) -> int:
    last_page_xpath = ".//li[@class='c-pager__item c-pager__last']/a/@href"
    last_page_link = h.xpath_string(doc, last_page_xpath)
    last_page_num = int(last_page_link.split("=")[-1])
    return last_page_num


def crawl_row(context: Context, row: dict[str, HtmlElement]) -> None:
    str_row = h.cells_to_str(row)

    # skip entities that have been withdrawn
    withdrawn_elem = row.pop("withdrawn")
    is_withdrawn = bool(withdrawn_elem.xpath('.//div[@class="featured"]'))
    if is_withdrawn is True:
        return

    company_elem = row.pop("company_sort_descending")
    company_link = h.xpath_string(company_elem, ".//a/@href")
    company_name = str_row.pop("company_sort_descending")

    # Create and emit an entity
    entity = context.make("Company")
    entity.id = context.make_id(company_name, company_link, prefix="ir-br-co")
    assert entity.id

    crawl_subpage(context, company_link, entity, entity.id)
    entity.add("name", company_name)
    entity.add("country", str_row.pop("nationality"))
    entity.add("sourceUrl", company_link)
    entity.add("ticker", str_row.pop("stock_symbol"))

    # FL 2026-02-13 - Legal work-around, do not remove without written approval
    if company_name is not None and "investment" in company_name.lower():
        entity.add("topics", "invest.risk")
    else:
        entity.add("topics", "export.risk")
    context.emit(entity)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    page_num = 0
    end_page = None

    while end_page is None or page_num <= end_page:
        doc = fetch_html(
            context,
            url=f"{context.data_url}?page={page_num}",
            unblock_validator=".//div[@class='o-grid']",
            geolocation="us",
            absolute_links=True,
        )
        if end_page is None:
            end_page = get_end_page(doc)

        table = h.xpath_element(doc, ".//div[@class='view-content']//table")

        for row in h.parse_html_table(table, skiprows=1):
            crawl_row(context, row)

        page_num += 1
