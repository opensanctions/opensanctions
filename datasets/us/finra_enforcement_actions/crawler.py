import re
import requests
from typing import Generator, Dict, Tuple, Optional
from lxml.etree import _Element
from normality import slugify, collapse_spaces
from zavod import Context, helpers as h

BASE_URL = "https://www.finra.org"


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
            a = el.find(".//a")
            if a is None:
                cells.append((collapse_spaces(el.text_content()), None))
            else:
                cells.append((collapse_spaces(a.text_content()), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}, row


def crawl_item(input_dict: dict, html_element: _Element, context: Context):
    schema = "LegalEntity"

    # We try to find if it's a person or company using the icon class
    if "user" in html_element.find(".//i").get("class"):
        schema = "Person"

    if "building" in html_element.find(".//i").get("class"):
        schema = "Company"

    name = input_dict.pop("firms-individuals")[0]
    case_summary = input_dict.pop("case-summary")[0]
    case_id = input_dict.pop("case-id")[0]
    source_url = BASE_URL + case_id
    date = input_dict.pop("action-date-sort-ascending")[0]
    document_type = input_dict.pop("document-type")[0]

    entity = context.make(schema)
    entity.id = context.make_slug(name)

    entity.add("name", name)
    entity.add("topics", "crime.fin")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity, key=case_id)

    sanction.add("description", case_summary)
    sanction.add("date", h.parse_date(date, formats=["%m/%d/%Y"]))
    sanction.add("sourceUrl", source_url)

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    # Each page only displays 15 rows at a time, so we need to loop until we find an empty table

    base_url = context.data_url

    page_num = 36

    while True:
        context.log.info(page_num)
        url = base_url + "?page=" + str(page_num)
        response = context.fetch_html(url)
        table = response.find(".//table")

        if table is None:
            break

        for item, html_element in parse_table(table):
            context.log.info("ok")
            crawl_item(item, html_element, context)

        page_num += 1
