from typing import Generator, Dict
import datetime
from lxml.etree import _Element
from lxml import html
import re
from zavod import Context, helpers as h
from normality import collapse_spaces

REGEX_URLS = r"(https?://[^\s]+)"


def parse_table(table: _Element) -> Generator[Dict[str, str], None, None]:
    """
    Parse the table and returns the information as a list of dict

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = [th.text_content() for th in table.findall(".//*/th")]
    for row in table.findall(".//*/tr")[1:]:
        cells = []
        for el in row.findall(".//td"):
            cells.append(collapse_spaces(el.text_content()))
        assert len(cells) == len(headers)

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("Name of unauthorised entities/individual")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)

    entity.add("name", name)
    entity.add("topics", "poi")

    # There can be multiple websites for each entity
    properties_text = input_dict.pop("Website")
    for website in re.findall(REGEX_URLS, properties_text):
        entity.add("website", website)
    entity.add("notes", properties_text)

    sanction = h.make_sanction(context, entity)

    # The date is always in the format %Y/%m/00%d %b %Y. For example: 2022/09/0029 Sep 2022
    sanction.add(
        "startDate",
        h.parse_date(
            input_dict.pop("Date Added to Alert List").split(" ")[0],
            formats=["%Y/%m/00%d"],
        ),
    )

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    if datetime.datetime.now() > datetime.datetime(2024, 9, 16):
        context.log.warn("Check if there's an update of the data behind the bot check.")

    data_path = context.dataset.base_path / "data.html"
    doc = html.fromstring(data_path.read_text())

    for item in parse_table(doc.find(".//table")):
        crawl_item(item, context)
