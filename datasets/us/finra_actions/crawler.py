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
from typing import Generator, Dict, Tuple, Optional
from lxml.etree import _Element
from normality import slugify
from zavod import Context, helpers as h


def parse_table(
    table: _Element,
) -> Generator[Dict[str, Tuple[str, Optional[str]]], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                headers.append(slugify(el.text_content()))
            continue

        cells = []
        for el in row.findall("./td"):
            for span in el.findall(".//span"):
                # add newline to split spans later if we want
                span.tail = "\n" + span.tail if span.tail else "\n"

            a = el.find(".//a")
            if a is None:
                cells.append((el.text_content(), None))
            else:
                cells.append((el.text_content(), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    schema = "LegalEntity"
    names = []
    for dirty_name in input_dict.pop("firms-individuals")[0].split("\n"):
        # for one known instance of ,1 at the end of the name
        dirty_name = re.sub(r",1$", "", dirty_name)
        names.extend(h.split_comma_names(context, dirty_name))
    case_summary = input_dict.pop("case-summary")[0].strip()
    case_id, source_url = input_dict.pop("case-id")
    date = input_dict.pop("action-date-sort-ascending")[0].strip()

    for name in names:
        entity = context.make(schema)
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("topics", "reg.action")
        entity.add("country", "us")
        context.emit(entity)

        sanction = h.make_sanction(context, entity, key=case_id)
        description = f"{date}: {case_summary}"
        sanction.add("description", description)
        sanction.add("authorityId", case_id.strip())
        sanction.add("sourceUrl", source_url)
        h.apply_date(sanction, "date", date)
        context.emit(sanction)

    context.audit_data(input_dict, ignore=["document-type"])


def crawl(context: Context):
    # Each page only displays 15 rows at a time, so we need to loop until we find an empty table
    page_num = 0
    while True:
        context.log.info(f"Crawling page {page_num}")
        url = context.data_url + "?page=" + str(page_num)
        # Caching for longer than 1 day can easily lead to missing entries as
        # the new stuff show up on the first page, and cached pages pages won't
        # include the stuff that were shifted off the previous uncached page.
        response = context.fetch_html(url, cache_days=1)
        response.make_links_absolute(url)
        table = response.find(".//table")

        if table is None:
            context.log.info("No table found")
            break
        if response.find(".//div[@class='view-empty']") is not None:
            context.log.info("Reached empty state")
            break

        for item in parse_table(table):
            crawl_item(item, context)

        page_num += 1
        if page_num > 3000:
            raise Exception("Too many pages")
