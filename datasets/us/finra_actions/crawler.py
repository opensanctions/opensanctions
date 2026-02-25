"""
# Occasional issues:

## Crawl completes but fewer than asserted entities are emitted.

Running the crawler locally the next day results in the expected number
of entities being emitted.

The crawl runs to the same number of pages (1231 in querystring) as usual.
It's not clear whether entities are removed and then new entities added by
the time we check the issue, or whether there's a bug. Keeping an eye on this
for a bit longer (2024-07-31)
"""

import re
from urllib.parse import urlparse, parse_qs
from typing import Dict, Optional

from lxml.etree import _Element

from zavod import Context, helpers as h


def crawl_item(context: Context, row: Dict[str, _Element]) -> None:
    names = []
    name_els = row.pop("firms_individuals")
    assert name_els is not None
    for dirty_name_el in name_els.findall(".//span"):
        # for one known instance of ,1 at the end of the name
        dirty_name = re.sub(r",1$", "", h.element_text(dirty_name_el))
        names.extend(h.split_comma_names(context, dirty_name))
    case_summary = h.element_text(row.pop("case_summary"))
    case_id_el = row.pop("case_id")
    case_id = h.element_text(case_id_el)
    source_url = case_id_el.get("href")
    date = h.element_text(row.pop("action_date_sort_ascending"))

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)
        entity.add("name", name)
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
        # Caching for longer than 1 day can easily lead to missing entries as
        # the new stuff show up on the first page, and cached pages won't
        # include the stuff that were shifted off the previous uncached page.
        response = context.fetch_html(url, cache_days=1, absolute_links=True)

        # Update max_page each iteration in case pagination changes.
        new_max = get_max_page(response)
        if new_max is not None:
            max_page = new_max

        table = response.find(".//table")
        if table is None:
            context.log.info("No table found. Skipping page.", page_num=page_num)
            page_num += 1
            continue
        if response.find(".//div[@class='view-empty']") is not None:
            context.log.info("No results found. Skipping page.", page_num=page_num)
            page_num += 1
            continue

        for row in h.parse_html_table(table):
            crawl_item(context, row)

        page_num += 1
