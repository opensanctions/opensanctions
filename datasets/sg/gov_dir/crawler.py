import re
from typing import Optional
from lxml import etree
from lxml.html import HtmlElement
from normality import collapse_spaces
from followthemoney.types import registry

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


TITLE_REGEX = re.compile(
    r"^(Mr|MR|Ms|Miss|Mrs|Prof|Dr|Professor Sir|Professor|Er. Dr.|Er.|Ar.|Dr.|A/Prof|Clinical A/Prof|Adj A/Prof|Assoc Prof \(Ms\)|Er. Prof.| Er Prof|Justice|Venerable|Bishop)\.? (?P<name>.+)$"
)
DATA_URLS = [
    "https://www.sgdi.gov.sg/statutory-boards",
    "https://www.sgdi.gov.sg/ministries",
    "https://www.sgdi.gov.sg/organs-of-state",
]
SPOKEPERSONS_URL = "https://www.sgdi.gov.sg/spokespersons"
# A bit of a crawler-specific attempt at capturing expectations for specific pages
# because I'm not convinced the markup is going to be consistent across the site.
# There's already two ways of listing people - one under collapsible sections (section-toggle)
# and another one (section-info).
# Some pages have no officials listed and don't even link to sub-sections - they're just empty.
# So we can't require either of those across the board.
PAGE_EXPECTATIONS = {
    # A full deep hierarchy
    "https://www.sgdi.gov.sg/ministries/mse/statutory-boards/nea/departments/sppg/departments/otd": {
        "officials": 1,
        "peps": 1,
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
    # Specific committee members a customer's been flagged for missing
    "https://www.sgdi.gov.sg/ministries/mha/committees/pcrh": {
        "officials": 10,
        "peps": 10,
    },
    # A section-info example
    "https://www.sgdi.gov.sg/ministries/mccy/committees/nic": {
        "officials": 10,
        "peps": 10,
    },
    # A section-toggle example
    "https://www.sgdi.gov.sg/ministries/mccy": {
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


class CrawlState(object):
    """Track what's seen and categorised to help validate the crawl"""

    seen_urls = set()
    uncategorised = set()


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


def is_pep(context: Context, rank: str) -> bool | None:
    res = context.lookup("rank_default_pep_status", rank)
    if res:
        return res.is_pep
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
    }, hierarchy[1]
    hierarchy.pop(0)  # Home
    hierarchy.pop(0)  # Ministries etc
    hierarchy.pop()  # Current page
    hierarchy.reverse()
    if len(hierarchy) > 1:
        return ", ".join(hierarchy)
    return None


def check_expectations(context: Context, link, official_count, pep_count, expectations):
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


def crawl_person(
    context: Context,
    state: CrawlState,
    official: HtmlElement,
    link: str,
    public_body,
    agency,
    section_name,
    hierarchy: Optional[str],
) -> bool:
    """Returns true if the crawled person is a PEP based on the provided position info"""

    rank = official.find(".//div[@class='rank']").text_content().strip()
    full_name = official.find(".//div[@class='name']").text_content().strip()
    email_elem = official.find(".//div[@class='email info-contact']")
    if email_elem is not None:
        email = email_elem.text_content().strip()
    else:
        # For spokepersons, the email is in a different format
        email = official.xpath(
            ".//div[@class='name']//span[@class='fas fa-envelope']/following-sibling::text()"
        )[0]

    pep_status = is_pep(context, rank)
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
                email = email.replace(" ", "").strip()
                # Silence warnings about broken emails, there are too many
                # TODO: Clean this up, see https://github.com/opensanctions/opensanctions/issues/1896
                email_clean = registry.email.clean(email)
                if email_clean is not None:
                    person.add("email", email)

    position = h.make_position(context, name=position_name, country="sg")
    categorisation = categorise(context, position, is_pep=pep_status)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)
            return True
    elif categorisation.is_pep is None:
        context.log.info("Uncategorised position", position=position_name, url=link)
        state.uncategorised.add(position_name)
    return False


def crawl_body(context: Context, state: CrawlState, link) -> None:
    """Crawl a government body page"""
    if link in state.seen_urls:
        return
    state.seen_urls.add(link)
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
    org_name = org_name_elem.text_content()
    org_parts = org_name.split("\n")
    public_body = org_parts[0].strip() if len(org_parts) > 0 else ""
    agency = org_parts[1].strip() if len(org_parts) > 1 else ""
    hierarchy = make_hierarchy(board_doc.find(".//span[@class='breadcrumbs']"))

    # Officials in headed sections
    sections = board_doc.xpath(".//div[contains(@class, 'section-toggle')]")
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
                context,
                state,
                official,
                link,
                public_body,
                agency,
                section_name,
                hierarchy,
            )
            if is_pep:
                pep_count += 1
    # Officials in section-info
    section_info = board_doc.findall(".//div[@class='section-info']")
    for section in section_info:
        for official in section.findall(".//li[@id]"):
            official_count += 1
            is_pep = crawl_person(
                context, state, official, link, public_body, agency, "", hierarchy
            )
            if is_pep:
                pep_count += 1

    check_expectations(context, link, official_count, pep_count, expectations)
    subdivision_links = board_doc.xpath(".//div[contains(@class, 'tab-content')]//a")
    for subdivision_link in subdivision_links:
        crawl_body(context, state, subdivision_link.get("href"))


def crawl_spokespersons(context: Context, state: CrawlState):
    """Crawl the root page to find all spokesperson links and process them."""
    main_doc = context.fetch_html(SPOKEPERSONS_URL, cache_days=1)

    for list_item in main_doc.xpath(".//ul[@class='contact-list']//a"):
        # Extract the public body from the text within an <a> element
        link_url = list_item.get("href")
        public_body = list_item.text_content().strip()
        crawl_subpage(context, state, link_url, public_body)


def crawl_subpage(
    context: Context, state: CrawlState, link: str, public_body, agency=None
):
    """Crawls a spokesperson page to extract spokesperson details and visit subdivision links."""
    if link in state.seen_urls:
        return
    state.seen_urls.add(link)
    subpage_doc = context.fetch_html(link)
    # Main subpage
    for person in subpage_doc.xpath("//div[@class='section-info']//li"):
        crawl_person(context, state, person, link, public_body, agency, "", None)
    # Subsections
    for subsection_el in subpage_doc.xpath(
        ".//div[@class='tab-content']//ul[@class='section-listing']//a"
    ):
        sub_link = subsection_el.get("href")
        agency = subsection_el.text_content().strip()
        if sub_link:
            crawl_subpage(context, state, sub_link, public_body, agency)


def crawl(context: Context):
    assert is_pep(context, "Director of this") is True
    assert is_pep(context, "Deputy director") is True
    assert is_pep(context, "DEADBEEF") is None
    assert is_pep(context, "Special Manager") is None
    assert is_pep(context, "Manager of x") is False
    assert is_pep(context, "PA to jimbo") is False

    state = CrawlState()
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
            crawl_body(context, state, link)
    # Crawl spokespersons pages
    crawl_spokespersons(context, state)
    expected = set(PAGE_EXPECTATIONS.keys())
    missing = expected - state.seen_urls
    if missing:
        context.log.warning("Expected URLs not crawled", missing=missing)
    if state.uncategorised:
        # TODO: Dial this up to warning once we've categorized them all,
        # see https://github.com/opensanctions/opensanctions/issues/2017
        context.log.info(
            "There are uncategorised positions for this crawler.",
            count=len(state.uncategorised),
        )
