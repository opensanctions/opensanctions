from typing import Generator, Dict, List, Tuple, Optional
from lxml.etree import _Element
from normality import slugify, collapse_spaces
from zavod import Context, helpers as h
import re

REGEX_AND = re.compile(r"(\band\b|&|\+)", re.I)
REGEX_JUST_A_NAME = re.compile(r"^[a-z]+, [a-z]+$", re.I)
REGEX_CLEAN_SUFFIX = re.compile(r", \b(LLC|L\.L\.C|Inc|Jr|INC|L\.P|LP|Sr|III|II|IV|S\.A|LTD|USA INC|\(?A/K/A|\(?N\.K\.A|\(?N/K/A|\(?F\.K\.A|formerly known as|INCORPORATED)\b", re.I)


def clean_names(context: Context, text: str) -> List[str]:
    text = collapse_spaces(text)
    if not text:
        return []
    
    text = REGEX_CLEAN_SUFFIX.sub(r" \1", text)
    # If the string ends in a comma, the last comma is unnecessary (e.g. Goldman Sachs & Co. LLC,)
    if text.endswith(","):
        text = text[:-1]
    
    if not REGEX_AND.search(text) and not REGEX_JUST_A_NAME.match(text):
        names = [n.strip() for n in text.split(",")]
        return names
    else:
        if "," in text:
            res = context.lookup("comma_names", text)
            if res:
                return res.names
            else:
                context.log.warning("Not sure how to split on comma.", text=text.lower())
                return [text]
        else:
            return [text]


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
                cells.append((a.text_content(), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    schema = "LegalEntity"
    names = []
    for dirty_name in input_dict.pop("firms-individuals")[0].split("\n"):
        names.extend(clean_names(context, dirty_name))
    case_summary = input_dict.pop("case-summary")[0]
    case_id, source_url = input_dict.pop("case-id")
    date = input_dict.pop("action-date-sort-ascending")[0]

    for name in names:
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
