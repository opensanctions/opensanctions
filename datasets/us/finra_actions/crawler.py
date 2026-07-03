"""
# Occasional issues:

FINRA listing pages are newest-first and served through inconsistent caches.
Intermediate pages can temporarily render the "No results found" empty state
even though reruns later return rows; pagination can also drift if the listing
changes while a crawl is in progress.

The Zyte fetch validator requires a populated table and rejects the empty-state
marker so these pages are retried and, if persistent, abort the crawl instead
of emitting a partial run. The crawler also aborts if the advertised last page
changes after pagination has been established.
"""

from lxml.etree import _Element
from typing import Dict, Optional
from urllib.parse import parse_qs, urljoin, urlparse

from zavod import Context, helpers as h
from zavod.extract import zyte_api

RESULT_ROW_VALIDATOR = (
    ".//table[not(ancestor-or-self::*//div"
    "[contains(concat(' ', normalize-space(@class), ' '), ' view-empty ')])]//tr[td]"
)


def crawl_item(context: Context, row: Dict[str, _Element]) -> None:
    names = []
    name_els = row.pop("firms_individuals")
    for div_row in h.xpath_elements(name_els, ".//div[@class='row']"):
        # Select the text-bearing span directly, skipping the icon span which has no
        # text content. This avoids relying on positional span[2] and also prevents
        # the outer wrapper span from concatenating all descendant text into a single
        # corrupted string.
        name = h.xpath_string(div_row, "./span[normalize-space(text())]/text()").strip()
        if not name:
            context.log.warning("No name span found, page structure may have changed")
            continue
        names.extend(h.split_comma_names(context, name))
    case_summary = h.element_text(row.pop("case_summary"))
    case_id_el = row.pop("case_id")
    case_id = h.element_text(case_id_el)
    source_url = case_id_el.get("href")
    if source_url is not None:
        source_url = urljoin(context.data_url, source_url)
    date = h.element_text(row.pop("action_date_sort_ascending"))

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)

        # Catches names with embedded alias indicators, e.g.:
        # "Score Priority Corp. formerly known as Just2Trade Inc."
        # "CODA Markets Inc. (f/k/a PDQ ATS Inc.)"
        h.apply_reviewed_name_string(
            context,
            entity,
            string=name,
            llm_cleaning=True,
        )

        entity.add("topics", "reg.action")
        entity.add("country", "us")
        context.emit(entity)

        sanction = h.make_sanction(context, entity, key=case_id)
        description = f"{date}: {case_summary}"
        sanction.add("description", description)
        sanction.add("authorityId", case_id)
        sanction.add("sourceUrl", source_url)
        h.apply_date(sanction, "date", date)
        context.emit(sanction)

    context.audit_data(row, ignore=["document_type"])


def get_max_page(response: _Element) -> Optional[int]:
    links = h.xpath_elements(response, ".//a[contains(@title, 'Go to last page')]")
    if len(links) == 0:
        # Intermediate result pages incorrectly showing "No Results Found" have
        # no pagination links.
        return None
    assert len(links) == 1, len(links)
    href = links[0].get("href", "")
    params = parse_qs(urlparse(href).query)
    return int(params["page"][0])


def crawl(context: Context) -> None:
    # Each page only displays 15 rows at a time. We determine the last page from
    # the pagination buttons because intermediate pages may report no results even
    # when later pages still have data.
    page_num = 0
    max_page = None
    while max_page is None or page_num <= max_page:
        context.log.info(f"Crawling page {page_num} of {max_page}")
        url = context.data_url + "?page=" + str(page_num)
        # Zyte because occasional cloudflare javascript challenge.
        response = zyte_api.fetch_html(
            context, url, RESULT_ROW_VALIDATOR, absolute_links=True
        )

        # Check the page count each iteration in case pagination changes.
        new_max = get_max_page(response)
        if new_max is not None:
            if max_page is None:
                max_page = new_max
            elif new_max != max_page:
                raise RuntimeError(
                    "FINRA pagination changed during crawl: "
                    f"expected last page {max_page}, got {new_max} "
                    f"on page {page_num}"
                )

        table = response.find(".//table")
        assert table is not None, "Validated FINRA page did not contain a table"

        for row in h.parse_html_table(table):
            crawl_item(context, row)

        page_num += 1
