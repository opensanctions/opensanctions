from dataclasses import dataclass
from typing import Dict, Iterator
from lxml import etree
import re
from zavod import Context, helpers as h

PROHIBITIONS_URL = "https://www.gfsc.gg/commission/enforcement/prohibitions"

REGEX_DETAILS = re.compile(
    r"^(?P<name>.*?)\s*\(?\s*[Dd]ate of Birth\s*(?P<dob>(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{2}/\d{2}/\d{4}))\s*\)?\s*(?:last known address\s*)?of\s+(?P<address>.*)$"
)


@dataclass
class ProhibitionDetails:
    name: str
    birth_date: str
    address: str
    prohibition_details: str


def parse_prohibition_from_html(
    context: Context, doc: etree._Element
) -> Iterator[ProhibitionDetails]:
    items = h.xpath_elements(
        doc, './/details[contains(@class, "helix-item-accordion")]'
    )
    if not items:
        raise Exception("Cannot find any details")

    for item in items:
        # Extract the summary and content details
        summary = item.find('.//summary/h3[@class="item-title"]')
        content = item.find('.//div[@class="generic-content field--name-copy"]')

        # Extract name, DOB, and address
        name_info = h.element_text(summary)
        title_match = re.search(REGEX_DETAILS, name_info)
        if not title_match:
            context.log.warning(
                f"Cannot extract name, date of birth, and address from {name_info}"
            )
            continue

        name = title_match.group("name").strip()
        birth_date = title_match.group("dob").strip()
        address = title_match.group("address").strip()

        # Extract prohibition details
        prohibition_details = h.element_text(content)

        yield ProhibitionDetails(name, birth_date, address, prohibition_details)


def crawl_prohibition(context: Context, item: ProhibitionDetails) -> None:
    name = item.name

    # Check for title and set gender
    title_match = re.match(r"(Mr|Mrs|Ms)\s+(?P<name>.+)", name)
    address = item.address
    gender = None
    if title_match:
        title = title_match.group(1)
        name = title_match.group("name")  # Remove title from the name
        if title == "Mr":
            gender = "male"
        elif title in ["Mrs", "Ms"]:
            gender = "female"

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    # Add gender if detected
    if gender:
        person.add("gender", gender)
    h.apply_date(person, "birthDate", item.birth_date)
    person.add("address", address)
    if "Guernsey" in address:
        person.add("country", "gg")
    person.add("notes", item.prohibition_details)
    person.add(
        "program", "Prohibition Orders by the Guernsey Financial Services Commission"
    )
    person.add("topics", "corp.disqual")
    context.emit(person)


def crawl_item(context: Context, item: Dict[str, str | None]) -> None:
    name = item.pop("name_of_disqualified_director")

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("country", "gg")
    person.add(
        "program",
        "Disqualified Directors by the Guernsey Financial Services Commission",
    )

    sanction = h.make_sanction(context, person)
    sanction.add("authority", item.pop("applicant_for_disqualification"))
    sanction.add("duration", item.pop("period_of_disqualification"))
    h.apply_date(sanction, "startDate", item.pop("date_of_disqualification"))
    h.apply_date(sanction, "endDate", item.pop("end_of_disqualification_period"))

    is_disqualified = h.is_active(sanction)
    if is_disqualified:
        person.add("topics", "corp.disqual")

    context.emit(person)
    context.emit(sanction)

    context.audit_data(item)


def crawl(context: Context) -> None:
    # Fetch and process the HTML from the main data URL
    response = context.fetch_html(context.data_url)
    for item in h.parse_html_table(h.xpath_element(response, ".//table")):
        crawl_item(context, h.cells_to_str(item))

    # Fetch and process the HTML for prohibitions
    prohibitions = context.fetch_html(PROHIBITIONS_URL)
    for prohibition_item in parse_prohibition_from_html(context, prohibitions):
        crawl_prohibition(context, prohibition_item)
