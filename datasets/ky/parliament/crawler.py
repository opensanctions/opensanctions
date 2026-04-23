import csv
import re
from typing import Dict, Optional
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.util import Element
from zavod.entity import Entity
from zavod.stateful.positions import categorise


HISTORICAL_DATA_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRM-hl1drkQMn4wHXxHkHXHZb2TkUyIFWxGXwJ_UqZNRpH00DGWrdHk_zDrHUsZ4YCeVYYojqSMmZuX/pub?output=csv"
TOPICS = ["gov.national"]

IGNORE_HEADINGS = {
    "About",
    "Members",
    "Business",
    "Hansard",
    "CPA",
    "PMC",
    "Media",
    "Official Term Photo",
}
KEEP_HEADINGS = {
    "Speaker": "Speaker",
    "Premier": "Premier of the Cayman Islands",
    "Cabinet Ministers": "Cabinet Minister",
    "Ex-Officio Members": "Ex-Officio Member of parliament",
    "Government Members": "Government Member of parliament",
    "Opposition Members": "Opposition Member of parliament",
}
REGEX_POSITIONISH = re.compile(
    r"(Minister|Attorney|Governor|Member|Parliamentary|Leader|Speaker)"
)
REGEX_NAME = re.compile(r"^[\w\.“”’-]+( [\w\.“”’-]+){1,3}$")
HONORIFICS = ["Hon. ", "Hon ", "Ms. ", "Mr. ", "Mrs. ", "Sir ", "Dr. "]
NAMES_ALLOW_LIST = ["A. Royston (Roy) Tatum", "Johany S. (Jay) Ebanks"]


def clean_name(context: Context, name: str) -> str:
    name = re.sub(r",.+", "", name)
    for honorific in HONORIFICS:
        name = name.replace(honorific, "")
    if not REGEX_NAME.match(name) and name not in NAMES_ALLOW_LIST:
        context.log.warning("Name doesn't look like a name", name=name)
    return name.strip()


def crawl_card_2025(
    context: Context, position_str: str, el: Element
) -> Optional[Entity]:
    content_el = h.xpath_element(el, ".//div[@class='member-select-content']")
    links_el = h.xpath_element(el, ".//div[@class='member-select-links']")
    name = clean_name(context, h.xpath_string(content_el, "./h1/text()"))
    entity = context.make("Person")
    entity.id = context.make_id(name)
    # BOTC and Caymanian: https://portal.elections.ky/index.php/candidates-agents/qualifications-for-candidates
    entity.add("citizenship", "KY")
    entity.add("name", name)
    personal_page_url = h.xpath_string(
        links_el, ".//p[@class='change-add-to-cart-text']/a/@href"
    )
    entity.add("sourceUrl", personal_page_url)

    position_detail = h.xpath_string(content_el, ".//p/text()")
    if REGEX_POSITIONISH.search(position_detail):
        entity.add("position", position_detail)
    else:
        context.log.warning(
            "Position detail value doesn't look like position detail",
            value=position_detail,
        )

    position = h.make_position(
        context,
        position_str,
        topics=TOPICS,
        country="ky",
    )
    categorisation = categorise(context, position, True)
    if not categorisation.is_pep:
        return None
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        period_start="2025",
        period_end="2029",
        categorisation=categorisation,
    )
    constituency = h.xpath_strings(
        links_el, ".//p[@class='change-add-to-cart-text']/text()"
    )
    if occupancy is not None:
        occupancy.add("constituency", constituency)
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)
        return entity
    return None


def crawl_row(context: Context, row: Dict[str, str]) -> None:
    start_date = row.pop("Start date", None)
    if start_date and start_date < h.earliest_term_start(TOPICS):
        context.log.info(
            f"Skipping row with start date {start_date} outside coverage window"
        )
        return

    entity = context.make("Person")
    name = row.pop("Name")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("citizenship", "KY")
    entity.add("title", row.pop("Title"))

    position = h.make_position(
        context,
        row.pop("Position"),
        topics=TOPICS,
        country="ky",
    )
    categorisation = categorise(context, position, True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        end_date=row.pop("End date"),
        period_start=row.pop("Period Start"),
        period_end=row.pop("Period End"),
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    # crawl_card assumes 2025-2029 members are current
    # update  period_start and period_end in crawl_card if this changes
    assert "2025-2029 Members" in h.xpath_strings(doc, ".//h3/text()")
    expected_current_member_count = 22
    current_member_count = 0
    heading = None
    for section in h.xpath_elements(doc, ".//section"):
        heading_str = h.xpath_strings(
            section, ".//h2[contains(@class, 'elementor-heading-title')]/text()"
        )
        if heading_str:
            heading = heading_str[0]
            if heading in IGNORE_HEADINGS:
                heading = None
                continue
            if heading in KEEP_HEADINGS:
                heading = KEEP_HEADINGS[heading]
            else:
                context.log.warn("unknown heading", heading=heading)
                heading = None
        else:
            if heading is None:
                continue
            for el in h.xpath_elements(
                section, ".//div[contains(@class, 'member-select-main')]"
            ):
                if crawl_card_2025(context, heading, el):
                    current_member_count += 1
    if current_member_count < 20:
        context.log.warning(
            f"Expected at least {expected_current_member_count} current members but found {current_member_count}"
        )
    # Former members sourced from https://parliament.ky/members/former-members/ via a
    # historical Google Sheet. Some individuals appear multiple times to reflect distinct
    # roles held across different periods.
    #
    # Date precision varies: start/end dates are exact where provided; for entries with
    # only a term reference (2021–2025), period start/end dates are used. Where a role
    # ended at "end of term", e.g. “November 23, 2023–end of term”, the end of that term,
    # i.e. 2025 is assumed as the `end_date`.
    path = context.fetch_resource("historical_data.csv", HISTORICAL_DATA_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
