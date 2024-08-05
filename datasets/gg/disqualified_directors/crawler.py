from typing import Generator, Dict
from lxml.etree import _Element
from normality import collapse_spaces
from datetime import datetime
from lxml import etree
import re
from zavod import Context, helpers as h

prohibitions_url = "https://www.gfsc.gg/commission/enforcement/prohibitions"


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


def parse_html(doc: etree.ElementTree):
    items = doc.findall('.//details[@class="helix-item helix-item-accordion"]')
    if not items:
        raise ValueError("Cannot find any details")

    for item in items:
        # Extract the summary and content details
        summary = item.find('.//summary/h3[@class="item-title"]')
        content = item.find('.//div[@class="generic-content field--name-copy"]')

        # Extract name, DOB, and address
        name_info = summary.text.strip()
        title_match = re.search(
            r"^(?P<name>.*?)\s*\(?\s*[Dd]ate of Birth\s*(?P<dob>\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*\)?\s+of\s+(?P<address>.*)$",
            name_info,
        )
        if not title_match:
            continue

        name = title_match.group(1).strip()
        birth_date = title_match.group(2).strip()
        address = title_match.group(3).strip()

        # Extract prohibition details
        prohibition_info = content.xpath(".//text()")

        # Join all text within the given div element and normalize whitespace
        if prohibition_info:
            joined_text = " ".join(prohibition_info)
            prohibition_details = re.sub(r"\s+", " ", joined_text).strip()
        else:
            prohibition_details = ""

        if "," in prohibition_details:
            prohibition_details = f'"{prohibition_details}"'

        yield {
            "name": name,
            "birth_date": birth_date,
            "address": address,
            "prohibition_details": prohibition_details,
        }


def crawl_prohibitions(item: Dict[str, str], context: Context):
    name = item.pop("name")

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("birthDate", h.parse_date(item.pop("birth_date"), formats=["%d %B %Y"]))
    person.add("address", item.pop("address"))
    person.add("notes", item.pop("prohibition_details"))
    person.add(
        "program", "Prohibition Orders by the Guernsey Financial Services Commission"
    )
    person.add("topics", "corp.disqual")
    context.emit(person)


def crawl_item(item: Dict[str, str], context: Context):
    name = item.pop("Name of disqualified director")

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("country", "gg")
    person.add(
        "program",
        "Disqualified Directors by the Guernsey Financial Services Commission",
    )

    end_date = h.parse_date(
        item.pop("End of disqualification period"), formats=["%d.%m.%Y"]
    )

    if end_date and end_date[0] < datetime.now().isoformat():
        ended = True
    else:
        ended = False
        person.add("topics", "corp.disqual")

    sanction = h.make_sanction(context, person)
    sanction.add(
        "startDate",
        h.parse_date(item.pop("Date of disqualification"), formats=["%d.%m.%Y"]),
    )
    sanction.add("authority", item.pop("Applicant for disqualification"))
    sanction.add("duration", item.pop("Period of disqualification"))
    sanction.add(
        "endDate",
        end_date,
    )

    context.emit(person, target=not ended)
    context.emit(sanction)

    context.audit_data(item)


def crawl(context: Context) -> None:
    # Fetch and process the HTML from the main data URL
    response = context.fetch_html(context.data_url)
    for item in parse_table(response.find(".//table")):
        crawl_item(item, context)

    # Fetch and process the HTML for prohibitions
    prohibitions = context.fetch_html(prohibitions_url)
    for item in parse_html(prohibitions):
        crawl_prohibitions(item, context)
