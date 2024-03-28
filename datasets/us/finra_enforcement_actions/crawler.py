from typing import Generator, Dict, List, Tuple, Optional
from lxml.etree import _Element
from normality import slugify, collapse_spaces
from zavod import Context, helpers as h
import re

REGEX_AND = re.compile(r"(\band\b|&)", re.I)
REGEX_JUST_A_NAME = re.compile(r"^[a-z]+, [a-z]+$", re.I)


def clean_names(name: str) -> List[str]:
    if not name:
        return []
    
    matches = {
        ", LLC": " LLC",
        ", L.L.C": " L.L.C",
        ", Inc": " Inc",
        ", Jr": " Jr",
        ", JR": " JR",
        ", INC": " INC",
        ", L.P.": " L.P.",
        ", LP": " LP",
        ", Sr.": " Sr.",
        ", SR.": " SR.",
        ", II": " II",
        ", III": " III",
        ", IV": " IV",
        ", S.A.": " S.A.",
        ", LTD": " LTD",
        ", USA INC": " USA INC",
    }

    # We first remove the comma using a pattern match
    for old, new in matches.items():
        name = name.replace(old, new)

    # If the string ends in a comma, the last comma is unnecessary (e.g. Goldman Sachs & Co. LLC,)
    if name.endswith(","):
        name = name[:-1]
    
    if not REGEX_AND.search(name) and not REGEX_JUST_A_NAME.match(name):
        parts = [n.strip() for n in name.split(",")]
        if len(parts) > 1:
            print("      ".join(parts))
        return parts

    return [name]


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
                cells.append((collapse_spaces(el.text_content()), None))
            else:
                cells.append((collapse_spaces(a.text_content()), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    schema = "LegalEntity"
    names = []
    for dirty_name in input_dict.pop("firms-individuals")[0].split("\n"):
        names.extend(clean_names(dirty_name))
    case_summary = input_dict.pop("case-summary")[0]
    case_id, source_url = input_dict.pop("case-id")
    date = input_dict.pop("action-date-sort-ascending")[0]

    for name in names:
        print("name: %r" % name)
        entity = context.make(schema)
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("topics", "crime.fin")
        entity.add("country", "us")
        context.emit(entity, target=True)

        sanction = h.make_sanction(context, entity, key=case_id)
        sanction.add("description", case_summary)
        sanction.add("date", h.parse_date(date, formats=["%m/%d/%Y"]))
        sanction.add("sourceUrl", source_url)
        context.emit(sanction)

    context.audit_data(input_dict, ignore=["document-type"])


def crawl(context: Context):
    # Each page only displays 15 rows at a time, so we need to loop until we find an empty table

    base_url = context.data_url

    page_num = 0

    while True:
        url = base_url + "?page=" + str(page_num)
        response = context.fetch_html(url, cache_days=7)
        response.make_links_absolute(url)
        table = response.find(".//table")

        if table is None:
            break

        for item in parse_table(table):
            crawl_item(item, context)

        page_num += 1
