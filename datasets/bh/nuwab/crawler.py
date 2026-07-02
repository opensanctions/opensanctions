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

# Archive members are grouped into <div id="<governorate><year>"> blocks.
GEO_YEAR_RE = re.compile(r"^(?:southern|capital|northern|muharraq|central)(20\d{2})$")
# Honorifics and the "MP" label the site prepends to member names.
NAME_PREFIX_RE = re.compile(
    r"^(MP|His Excellency|Her Excellency|Mr\.|Mrs\.|Ms\.|Dr\.|Eng\.|Sheikh)\s+",
    re.IGNORECASE,
)


def parse_profile(block: _Element) -> tuple[str, str | None]:
    """Parse a member profile block (used for both detail pages and the
    speaker's block on the listing page), which share one template."""
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


def emit_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    entity_id: str | None,
    name: str,
    biography: str | None,
    terms: set[int],
    current_year: int,
) -> None:
    """Emit a person and one occupancy per term they served."""
    assert entity_id is not None
    person = context.make("Person")
    person.id = entity_id
    person.add("name", name, lang="eng")
    person.add("biography", biography, lang="eng")
    # The Constitution of Bahrain (2002), Article 57, requires a member of the
    # Council of Representatives to hold Bahraini citizenship:
    # https://www.lloc.gov.bh/en/page/The%20Constitution%20of%20the%20Kingdom%20of%20Bahrain
    person.add("citizenship", "bh")

    for year in terms:
        is_current = year == current_year
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            period_start=str(year),
            period_end=None if is_current else str(year + TERM_YEARS),
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(person)


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

    # Map each member's detail-page URL to the set of terms they served in.
    member_terms: dict[str, set[int]] = {}
    for url in member_links(listing):
        member_terms.setdefault(url, set()).add(current_year)

    archive = context.fetch_html(ARCHIVE_URL, cache_days=1)
    for block in h.xpath_elements(archive, "//div[@id]"):
        block_id = block.get("id")
        if block_id is None:
            continue
        match = GEO_YEAR_RE.match(block_id)
        if match is None:
            continue
        year = int(match.group(1))
        for url in member_links(block):
            member_terms.setdefault(url, set()).add(year)

    for url, terms in member_terms.items():
        detail = context.fetch_html(url, cache_days=14)
        block = h.xpath_element(
            detail,
            '//div[contains(@class, "content")]'
            '[./h4][.//ul[contains(@class, "contactInfo")]]',
        )
        name, biography = parse_profile(block)
        emit_member(
            context,
            position,
            categorisation,
            context.make_id(url),
            name,
            biography,
            terms,
            current_year,
        )

    # The Speaker is featured on the listing page without a detail-page link.
    speaker_block = h.xpath_element(
        listing,
        '//div[contains(@class, "content")]'
        '[./h4][.//ul[contains(@class, "contactInfo")]]',
    )
    name, biography = parse_profile(speaker_block)
    emit_member(
        context,
        position,
        categorisation,
        context.make_id("speaker", name),
        name,
        biography,
        {current_year},
        current_year,
    )
