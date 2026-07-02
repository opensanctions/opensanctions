import re

from lxml.etree import _Element

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

ARCHIVE_URL = "https://www.nuwab.bh/en/Archive-of-Member-of-Parliaments/"

# Elected terms run four years; the source identifies each term by its election
# year (2002, 2006, ...). The current term has no end date.
TERM_YEARS = 4

# The archive lists members by governorate constituency, one tab per governorate
# per term. All five ran until the Central Governorate was abolished in 2014, so
# terms from 2014 onward list only the other four.
GOVERNORATES = {"southern", "capital", "northern", "muharraq", "central"}
CENTRAL_ABOLISHED = 2014

# Honorifics and the "MP" label the site prepends to member names.
NAME_PREFIX_RE = re.compile(
    r"^(MP|His Excellency|Her Excellency|Mr\.|Mrs\.|Ms\.|Dr\.|Eng\.|Sheikh)\s+",
    re.IGNORECASE,
)
# The member profile template, used both on detail pages and for the speaker's
# block on the listing page.
PROFILE_BLOCK = (
    '//div[contains(@class, "content")][./h4][.//ul[contains(@class, "contactInfo")]]'
)


def parse_profile(block: _Element) -> tuple[str, str | None]:
    """Parse a member profile block into its name and biography."""
    raw_name = h.element_text(h.xpath_element(block, "./h4"))
    name = NAME_PREFIX_RE.sub("", raw_name)
    paragraphs = [h.element_text(p) for p in h.xpath_elements(block, "./p")]
    biography = "\n".join(p for p in paragraphs if p) or None
    return (name, biography)


def member_links(container: _Element) -> set[str]:
    """Collect the detail-page URLs of all members linked within an element."""
    links = set()
    for anchor in h.xpath_elements(container, './/a[contains(@href, "/mps/")]'):
        href = anchor.get("href")
        if href is not None:
            links.add(href.split("?")[0])
    return links


def get_current_year(listing: _Element) -> int:
    """Read the current term's election year from the active navigation label
    (e.g. "MPs 2022"), so the crawler tracks new terms without code changes."""
    for anchor in h.xpath_elements(
        listing, '//a[contains(@href, "members-of-parliament")]'
    ):
        match = re.search(r"20\d{2}", h.element_text(anchor))
        if match is not None:
            return int(match.group(0))
    raise RuntimeError("Could not determine the current legislative term year")


def crawl_person(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    entity_id: str | None,
    source_url: str | None,
    block: _Element,
    year: int,
    is_current: bool,
) -> None:
    """Make a member from their profile block and emit them with an occupancy
    for the given term, if that occupancy qualifies as a PEP position."""
    name, biography = parse_profile(block)
    person = context.make("Person")
    person.id = entity_id
    person.add("name", name, lang="eng")
    person.add("biography", biography, lang="eng")
    person.add("sourceUrl", source_url)
    # The Constitution of Bahrain (2002), Article 57, requires a member of the
    # Council of Representatives to hold Bahraini citizenship:
    # https://www.lloc.gov.bh/en/page/The%20Constitution%20of%20the%20Kingdom%20of%20Bahrain
    person.add("citizenship", "bh")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=str(year),
        period_end=None if is_current else str(year + TERM_YEARS),
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(occupancy)


def crawl_members(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    links: set[str],
    year: int,
    is_current: bool,
) -> None:
    """Fetch each member's detail page and emit them for the given term."""
    for url in links:
        detail = context.fetch_html(url, cache_days=14)
        block = h.xpath_element(detail, PROFILE_BLOCK)
        crawl_person(
            context,
            position,
            categorisation,
            context.make_id(url),
            url,
            block,
            year,
            is_current,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of Representatives",
        country="bh",
        wikidata_id="Q21328582",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    listing = context.fetch_html(context.data_url, cache_days=1)
    current_year = get_current_year(listing)

    # Current term: the live roster, plus the Speaker who is featured on the
    # listing page without a detail-page link.
    crawl_members(
        context, position, categorisation, member_links(listing), current_year, True
    )
    speaker_block = h.xpath_element(listing, PROFILE_BLOCK)
    speaker_name, _ = parse_profile(speaker_block)
    # The Speaker has no detail page, so there is no deep link to record.
    crawl_person(
        context,
        position,
        categorisation,
        context.make_id("speaker", speaker_name),
        None,
        speaker_block,
        current_year,
        True,
    )

    # Historical terms: one governorate tablist per term in the archive.
    archive = context.fetch_html(ARCHIVE_URL, cache_days=1)
    for tablist in h.xpath_elements(archive, '//ul[@id="tabs"]'):
        panel_ids = [
            tab.get("href", "").removeprefix("#")
            for tab in h.xpath_elements(tablist, "./li/a")
        ]
        year = int(panel_ids[0][-4:])
        # Assert the term lists exactly the governorates we expect (minus Central
        # once abolished), so a dropped or added tab crashes rather than silently
        # under-collecting a term.
        expected = GOVERNORATES
        if year >= CENTRAL_ABOLISHED:
            expected = GOVERNORATES - {"central"}
        governorates = {panel_id.rstrip("0123456789") for panel_id in panel_ids}
        assert governorates == expected, (year, expected ^ governorates)
        for panel_id in panel_ids:
            panel = h.xpath_element(archive, f'//div[@id="{panel_id}"]')
            crawl_members(
                context, position, categorisation, member_links(panel), year, False
            )
