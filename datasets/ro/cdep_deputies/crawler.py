import re
import time

from lxml.etree import _Element
from requests.exceptions import RequestException

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

TOPICS = ["gov.national", "gov.legislative"]

# cdep.ro intermittently drops connections (a broken TLS chain plus rate-limiting on
# rapid bursts), so a single sequential pass over thousands of pages will hit transient
# errors. Retry with backoff, and rely on a long cache so re-runs resume from where a
# previous pass left off.
CACHE_DAYS = 14

# Dates are embedded in labelled snippets like "b. 14 oct. 1988" or
# "start of the mandate: 21 december 2024", so we isolate the date substring here.
# Translating the month name and parsing the format (including the abbreviated
# month's trailing period) is left to `h.apply_date` via the dataset `dates` config.
DATE_RE = re.compile(r"\d{1,2}\s+[A-Za-z]+\.?\s+\d{4}")

# Each member links to a detail page like
# /ords/pls/parlam/structura2015.mp?idm=1&cam=2&leg=2024&idl=2
# `idm` is an index *within a legislature*, not a stable person ID, so a deputy who
# served several terms is emitted once per term under a distinct (leg, idm) ID and
# merged downstream by entity resolution.
MEMBER_XPATH = ".//a[contains(@href, 'structura2015.mp?idm=')]"
IDM_RE = re.compile(r"[?&]idm=(\d+)")
# The roster page also links to every legislature's home page; we read the full set
# of term years from these so the crawler follows new terms automatically.
LEG_XPATH = ".//a[contains(@href, 'structura2015.home?leg=')]"
LEG_RE = re.compile(r"leg=(\d{4})")
NAME_XPATH = ".//h1[@class='mp-profile-name2025']"

ROSTER_URL = "https://www.cdep.ro/ords/pls/parlam/structura2015.de?leg=%s&idl=2"


class InvalidMemberPage(Exception):
    pass


def fetch_html(
    context: Context,
    url: str,
    absolute_links: bool = False,
    attempts: int = 6,
    required_xpath: str | None = None,
) -> _Element:
    """Fetch and parse an HTML page, retrying transient bad responses.

    cdep.ro frequently resets connections mid-crawl; a plain `context.fetch_html`
    aborts the whole run on the first drop. This backs off and retries so the crawl
    survives the flaky server. When a marker XPath is supplied, bad cached or
    interstitial pages are rejected and refetched before the member is skipped.
    """
    for attempt in range(1, attempts + 1):
        try:
            doc = context.fetch_html(
                url, absolute_links=absolute_links, cache_days=CACHE_DAYS
            )
            if required_xpath is not None:
                try:
                    h.xpath_elements(doc, required_xpath, expect_exactly=1)
                except ValueError as exc:
                    context.clear_url(url)
                    raise InvalidMemberPage(str(exc)) from exc
            return doc
        except RequestException as exc:
            if attempt == attempts:
                raise
            pause = 2**attempt
            context.log.info(
                "Retrying after connection error",
                url=url,
                attempt=attempt,
                pause=pause,
                error=str(exc),
            )
            time.sleep(pause)
        except InvalidMemberPage as exc:
            if attempt == attempts:
                raise
            pause = 2**attempt
            context.log.info(
                "Retrying after invalid member page",
                url=url,
                attempt=attempt,
                pause=pause,
                error=str(exc),
            )
            time.sleep(pause)
    raise RuntimeError("unreachable")


def date_in_text(text: str | None) -> str | None:
    """Return the date substring (e.g. "14 oct. 1988") found in a labelled snippet.

    Returns None when no date is present, so `apply_date` is never handed the
    surrounding label text (which it cannot parse and would emit verbatim)."""
    if text is None:
        return None
    match = DATE_RE.search(text)
    return match.group(0) if match is not None else None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    leg_year: int,
    end_year: int | None,
    is_current: bool,
) -> None:
    match = IDM_RE.search(url)
    assert match is not None, url
    idm = match.group(1)

    try:
        doc = fetch_html(context, url, required_xpath=NAME_XPATH)
    except (RequestException, InvalidMemberPage) as exc:
        # A single member page that stays unreachable after all retries shouldn't
        # abort a crawl of thousands of pages; drop it and let the `min` assertions
        # catch any wholesale shortfall.
        context.log.warning(
            "Skipping member after repeated fetch failures", url=url, error=str(exc)
        )
        return

    name = h.element_text(h.xpath_element(doc, NAME_XPATH))
    person = context.make("Person")
    person.id = context.make_id(name, idm)
    person.add("name", name)
    person.add("sourceUrl", url)
    # Members of the Chamber of Deputies must be Romanian citizens: Constitution of
    # Romania, Article 37(1) (right to be elected), read with Article 16(3) (public
    # office reserved to Romanian citizens). https://www.ccr.ro/en/constitution-of-roumania/
    person.add("citizenship", "ro")

    birth_box = h.xpath_elements(
        doc,
        ".//div[contains(@class, 'mp-contact-item2025')][starts-with(normalize-space(.), 'b.')]",
    )
    if len(birth_box) == 1:
        h.apply_date(person, "birthDate", date_in_text(h.element_text(birth_box[0])))

    party_links = h.xpath_elements(
        doc,
        ".//div[contains(@class, 'mp-info-box2025')][h4[normalize-space(.)='Political party:']]//a",
    )
    for link in party_links:
        person.add("political", h.element_text(link))

    mandate = h.xpath_elements(
        doc,
        ".//div[contains(@class, 'mp-profile-details2025')]"
        "//li[contains(., 'start of the mandate')]",
    )
    # Older legislatures omit the precise validation date; fall back to the term year.
    start_date = None
    if len(mandate) == 1:
        start_date = date_in_text(h.element_text(mandate[0]))
    if start_date is None:
        start_date = str(leg_year)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=None if is_current else str(end_year),
        no_end_implies_current=is_current,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)


def crawl_roster(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    leg_year: int,
    end_year: int | None,
    is_current: bool,
    doc: _Element | None = None,
) -> None:
    if doc is None:
        roster = fetch_html(context, ROSTER_URL % leg_year, absolute_links=True)
    else:
        roster = doc
    members: dict[str, None] = {}
    for link in h.xpath_elements(roster, MEMBER_XPATH):
        href = link.get("href")
        assert href is not None, link
        members.setdefault(href, None)  # order-preserving de-duplication
    context.log.info("Crawling legislature roster", leg=leg_year, members=len(members))
    for url in members:
        crawl_member(
            context, position, categorisation, url, leg_year, end_year, is_current
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Romania",
        country="ro",
        wikidata_id="Q17556530",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    # The current roster lists current members and links to every legislature's home
    # page; we read the full set of term years from it.
    root = fetch_html(context, context.data_url, absolute_links=True)
    years: set[int] = set()
    for link in h.xpath_elements(root, LEG_XPATH):
        href = link.get("href")
        assert href is not None, link
        match = LEG_RE.search(href)
        assert match is not None, href
        years.add(int(match.group(1)))
    assert len(years) > 5, years
    ordered = sorted(years)
    current_year = ordered[-1]

    cutoff_year = int(h.earliest_term_start(TOPICS)[:4])

    for index, leg_year in enumerate(ordered):
        end_year = ordered[index + 1] if index + 1 < len(ordered) else None
        is_current = leg_year == current_year
        if not is_current and (end_year is None or end_year < cutoff_year):
            continue
        crawl_roster(
            context,
            position,
            categorisation,
            leg_year,
            end_year,
            is_current,
            doc=root if is_current else None,
        )
