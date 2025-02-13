import re
from typing import Generator, Optional
from lxml import etree
from lxml.html import HtmlElement
from normality import collapse_spaces

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


TITLE_REGEX = re.compile(
    r"^(Mr|MR|Ms|Miss|Mrs|Prof|Dr|Professor Sir|Professor|Er. Dr.|Er.|Ar.|Dr.|A/Prof|Clinical A/Prof|Adj A/Prof|Assoc Prof \(Ms\)|Er. Prof.| Er Prof|Justice)\.? (?P<name>.+)$"
)
DATA_URLS = [
    "https://www.sgdi.gov.sg/statutory-boards",
    "https://www.sgdi.gov.sg/ministries",
    "https://www.sgdi.gov.sg/organs-of-state",
]
# A sample of pages with an expected minimum number of (officials, PEPs)
# There are pages with no officials, so we can't assert that every page has some.
PAGE_EXPECTATIONS = {
    "https://www.sgdi.gov.sg/ministries/mse/statutory-boards/nea/departments/sppg/departments/otd": {
        "officials": 10,
        "peps": 10,
    },
    "https://www.sgdi.gov.sg/ministries/mse/statutory-boards/nea/departments/sppg": {
        "officials": 1,
        "peps": 1,
    },
    "https://www.sgdi.gov.sg/ministries/mse/statutory-boards/nea": {
        "officials": 20,
        "peps": 6,
    },
    "https://www.sgdi.gov.sg/ministries/mse": {
        "officials": 20,
        "peps": 6,
    },
    "https://www.DEADBEEF.gov.sg/ministries/mse/statutory-boards/nea/departments/sppg/departments/otd": {
        "officials": 10,
        "peps": 10,
    },
}

POSITION_REPLACEMENTS = [
    (re.compile(p, re.I), r)
    for p, r in [
        (r"Senior Statutory Board Officers & Their Personal Assistants,", ""),
        (r"Senior Management & Their Personal Assistants,", ""),
        (r"Political Appointees & Their Personal Assistants,", ""),
        (r"Political Appointees and Their Personal Assistants,", ""),
        (r"Member, Board Members,", "Member of the board,"),
        (r"Board Member of the board,", "Member of the board,"),
        (r"Member, Council Members", "Member of the Council"),
        (r"Chairman, Board Members", "Chairman of the Board"),
        (r"Vice President, Board Members", "Vice President of the Board"),
        (r"President, Board Members", "President of the Board"),
        (r"Mufti, Council Members,", "Mufti"),
        (r", , ", ", "),
    ]
]


def make_position_name(rank, public_body, agency, section_name, hierarchy):
    if agency:
        position = f"{rank}, {section_name}, {agency}"
    else:
        position = f"{rank}, {section_name}, {public_body}"
    if hierarchy:
        position = f"{position}, ({hierarchy})"
    for regex, replacement in POSITION_REPLACEMENTS:
        position = regex.sub(replacement, position)
    return collapse_spaces(position)


def is_pep(rank: str) -> bool | None:
    rank_lower = rank.lower()

    # Check for PEP indicators
    if any(
        keyword in rank_lower
        for keyword in [
            "minister",
            "permanent secretary",
            "president",
            "member",
            "executive officer",
            "director",
            "chairman",
            "ceo",
            "mayor",
            "auditor-general",
            "justice",
            "judge",
            "deputy chief",
            "deputy secretary",
            "senior parliamentary secretary",
            "chief prosecutor",
            "head of secretariat",
            "senior adviser",
            "senior executive",
            "senior auditor",
            "ambassador",
            "chief executive",
            "chief financial officer",
        ]
    ):
        return True
    else:
        return None


def make_hierarchy(breadcrumbs: HtmlElement) -> Optional[str]:
    """
    A string like "PEC, IPOS, MINLAW" if the depth is > 1

    Excludes the current entity. Intended to concisely show ancestors on positions
    nested deeply in the government hierarchy.
    """
    hierarchy = []
    for crumb in breadcrumbs.findall(".//a"):
        hierarchy.append(crumb.text_content())
    assert hierarchy[0] == "Home"
    assert hierarchy[1] in {
        "Ministries",
        "Organs of State",
        "Directory of Government Spokespersons",
    }, hierarchy[1]
    hierarchy.pop(0)  # Home
    hierarchy.pop(0)  # Ministries etc
    hierarchy.pop()  # Current page
    hierarchy.reverse()
    if len(hierarchy) > 1:
        return ", ".join(hierarchy)
    return None


def crawl_person(
    context: Context, official, link, public_body, agency, section_name, hierarchy
):
    rank = official.find(".//div[@class='rank']").text_content().strip()
    # Skip all non-PEP positions
    if any(
        keyword in rank.lower()
        for keyword in [
            "pa to",
            "assistant",
            "secretary to",
            "please contact",
            "quality service manager",
            "administrative professional",
        ]
    ):
        return
    full_name = official.find(".//div[@class='name']").text_content().strip()
    email = official.find(".//div[@class='email info-contact']").text_content().strip()
    # phone numbers are also available

    pep_status = is_pep(rank)  # checking before formatting the position name
    position_name = make_position_name(
        rank, public_body, agency, section_name, hierarchy
    )

    # Override status and position name for MPs
    if collapse_spaces(section_name.lower()) == "members of parliament":
        pep_status = True
        position_name = "Member of Parliament"
    match = TITLE_REGEX.match(full_name)
    title = None
    if match:
        full_name = match.group("name")
        title = match.group(1)

    person = context.make("Person")
    person.id = context.make_id(full_name, rank, agency, public_body)
    person.add("name", full_name)
    person.add("sourceUrl", link)
    person.add("position", rank)
    if title is not None:
        person.add("title", title)
    if email is not None:
        if email.startswith("https://"):
            person.add("website", email)
        else:
            for email in email.split("; "):
                person.add("email", email)

    position = h.make_position(context, name=position_name, country="sg")
    categorisation = categorise(context, position, is_pep=pep_status)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)
    elif categorisation.is_pep is None:
        context.log.warning("Uncategorised position", position=position_name, url=link)


def crawl_body(context: Context, link) -> Generator[str, None, None]:
    """Crawl a government body page and yield own and sub-page URLs"""
    expectations = PAGE_EXPECTATIONS.get(link, None)
    official_count = 0
    pep_count = 0
    board_doc = context.fetch_html(link, cache_days=1)

    org_name_elem = board_doc.find(".//div[@id='agencyName']/h1")
    for br in org_name_elem.xpath(".//br"):
        if br.tail is None:
            br.tail = "\n"
        else:
            br.tail = "\n" + br.tail
    # Get the text content and split it by newline
    org_name = org_name_elem.text_content()
    org_parts = org_name.split("\n")
    public_body = org_parts[0].strip() if len(org_parts) > 0 else ""
    agency = org_parts[1].strip() if len(org_parts) > 1 else ""

    hierarchy = make_hierarchy(board_doc.find(".//span[@class='breadcrumbs']"))

    sections = board_doc.xpath(".//div[contains(@class, 'section-toggle')]")

    # Iterate through sections and their officials
    for section in sections:
        headers = section.xpath(".//*[contains(@class, 'section-header')]")
        assert len(headers) == 1, etree.tostring(headers)
        if "porto-subtitle" in headers[0].get("class"):
            # Skip portfolio: a list of legislation related to the body
            continue
        section_name = headers[0].text_content().strip()
        # Identify positions related to the current section
        section_bodies = section.xpath(".//*[contains(@class, 'section-body')]")
        assert len(section_bodies) == 1, etree.tostring(section_bodies)
        officials = section_bodies[0].findall(".//li[@id]")
        for official in officials:
            official_count += 1
            is_pep = crawl_person(
                context, official, link, public_body, agency, section_name, hierarchy
            )
            if is_pep:
                pep_count += 1

    section_info = board_doc.findall(".//div[@class='section-info']")
    for section in section_info:
        for official in section.findall(".//li[@id]"):
            official_count += 1
            is_pep = crawl_person(
                context, official, link, public_body, agency, "", hierarchy
            )
            if is_pep:
                pep_count += 1

    if expectations:
        if official_count < expectations["officials"]:
            context.log.warning(
                "Insufficient officials",
                expected=expectations["officials"],
                actual=official_count,
                url=link,
            )
        if pep_count < expectations["peps"]:
            context.log.warning(
                "Insufficient PEPs",
                expected=expectations["peps"],
                actual=pep_count,
                url=link,
            )

    yield link
    subdivision_links = board_doc.xpath(".//div[contains(@class, 'tab-content')]//a")
    for subdivision_link in subdivision_links:
        yield from crawl_body(context, subdivision_link.get("href"))


def crawl(context: Context):
    crawled = set()
    for url in DATA_URLS:
        data_url = url
        doc = context.fetch_html(data_url, cache_days=1)
        bodies = doc.findall(
            ".//div[@class='directory-list']//ul[@class='ministries']//li//a"
        )
        for board in bodies:
            link = board.get("href")
            org_name = board.text_content().strip()
            if link is None:
                context.log.warning("No link found", org_name=org_name)
                continue
            if org_name == "":
                context.log.warning("No org name found", link=link)
                continue
            crawled.update(crawl_body(context, link))
    expected = set(PAGE_EXPECTATIONS.keys())
    missing = expected - crawled
    if missing:
        context.log.warning("Expected URLs not crawled", missing=missing)
