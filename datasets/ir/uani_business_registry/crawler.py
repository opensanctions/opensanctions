from urllib.parse import urljoin

from lxml import etree
from normality import slugify
from zavod.extract.zyte_api import fetch_html
from zavod.util import Element

from zavod import Context, Entity
from zavod import helpers as h


def parse_facts_list(container: Element) -> dict[str, list[Element]]:
    """
    Parse the company page's facts list into a dict keyed by slugified label.

    Values are the raw `<span>` cell elements so callers can extract text or
    links as appropriate for each field.
    """
    data: dict[str, list[Element]] = {}
    rows = h.xpath_elements(
        container, './/div[contains(@class, "c-full-node__info--row")]'
    )
    for row in rows:
        label = h.xpath_element(row, './/label[contains(@class, "field__label")]')
        key = slugify(h.element_text(label), sep="_")
        assert key, h.element_text(label)
        assert key not in data, (key, data)
        data[key] = h.xpath_elements(row, "./span")
    return data


def emit_related_company(context: Context, cell: Element) -> Entity:
    """Emit a Company referenced by name and link(s) in a facts-list cell."""
    # squash=False + strip() preserves the exact strings previously produced by
    # text_content().strip() — these feed make_id, so squashing internal
    # whitespace would change entity IDs.
    name = h.element_text(cell, squash=False).strip()
    # Some links on the source page are relative (e.g. /company/fortis-bank-sanv);
    # resolve them against the data URL so the url type cleaner accepts them and
    # entity IDs stay consistent. urljoin is a no-op on already-absolute URLs.
    urls = [
        urljoin(context.data_url, url) for url in h.xpath_strings(cell, ".//a/@href")
    ]
    company = context.make("Company")
    company.id = context.make_id(name, *urls, prefix="ir-br-co")
    assert company.id
    company.add("name", name)
    company.add("sourceUrl", urls)
    context.emit(company)
    return company


def crawl_subpage(context: Context, url: str, entity: Entity, entity_id: str) -> None:
    context.log.info("Crawling company page", url=url)
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
    # An exact @class match is deliberate here (and for the facts list below):
    # contains() would also match the c-full-node__info--row divs.
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
        entity.add("sector", h.element_text(industry, squash=False).strip())

    for sources in facts.pop("sources", []):
        for source in sources.findall(".//p"):
            # Sometimes, the tree contains some weird CSS elements
            # with something that looks like an HTML comment - get rid of those.
            etree.strip_elements(source, "style", "script")
            # squash=False: the type.text lookups in the .yml match the raw
            # text, non-breaking spaces included.
            source_text = h.element_text(source, squash=False)
            if "initWindowFocus" in source_text:
                continue
            for source_url in h.xpath_strings(source, ".//a/@href"):
                source_text += f" ({source_url})"
            entity.add("notes", source_text)

    for website in facts.pop("website", []):
        entity.add("website", h.xpath_strings(website, ".//a/@href"))

    for owner in facts.pop("parent_company", []):
        parent = emit_related_company(context, owner)
        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity_id, parent.id, prefix="ir-br-own")
        ownership.add("asset", entity.id)
        ownership.add("owner", parent.id)
        context.emit(ownership)

    # Most of the time this is the subsidiary, but JX Nippon Oil & Energy
    # and Japan Energy Corporation are only affiliates
    for affiliate in facts.pop("affiliates_subsidiaries", []):
        subsidiary = emit_related_company(context, affiliate)
        assert subsidiary.id

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
    last_page_xpath = ".//li[contains(@class, 'c-pager__last')]/a/@href"
    last_page_link = h.xpath_string(doc, last_page_xpath)
    last_page_num = int(last_page_link.split("=")[-1])
    return last_page_num


def crawl_row(context: Context, row: dict[str, Element]) -> None:
    str_row = h.cells_to_str(row)

    # skip entities that have been withdrawn
    withdrawn_cell = row.pop("withdrawn")
    withdrawn_text = str_row.pop("withdrawn")
    withdrawn_marker = h.xpath_elements(
        withdrawn_cell, './/div[contains(@class, "featured")]'
    )
    is_withdrawn = len(withdrawn_marker) > 0
    # Withdrawn status is only conveyed by the empty marker div, never as cell
    # text; if the cell starts carrying text, the column semantics have changed
    # and the marker check needs review.
    assert withdrawn_text is None, withdrawn_text
    if is_withdrawn:
        return

    company_elem = row.pop("company_sort_descending")
    company_link = h.xpath_string(company_elem, ".//a/@href")
    # The listing page HTML alternates between two href forms for the same company:
    # /company/fisher-scientific and /index.php/company/fisher-scientific.
    # Both resolve to the same page, but including the raw URL in make_id would
    # produce different entity IDs across runs depending on which form appeared.
    # Stripping /index.php/ canonicalises the path before hashing.
    company_link_clean = company_link.replace("/index.php/", "/")
    company_link_clean = urljoin(context.data_url, company_link_clean)
    company_name = str_row.pop("company_sort_descending")

    # Create and emit an entity
    entity = context.make("Company")
    entity.id = context.make_id(company_name, company_link_clean, prefix="ir-br-co")
    assert entity.id

    crawl_subpage(context, company_link_clean, entity, entity.id)
    entity.add("name", company_name)
    entity.add("country", str_row.pop("nationality"))
    entity.add("sourceUrl", company_link_clean)
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
