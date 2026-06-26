import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

MEMBERS_URL = "https://www.parliament.qld.gov.au/Members/Current-Members/Member-List"
# Each member is one card in the roster; the roster is repeated across grouped
# views on the page, so distinct members are keyed by their detail-page id.
CARD = "//div[contains(@class, 'member-listing__profile-content')]"
NAME = ".//span[contains(@class, 'member-listing__member-name')]"
LINK = ".//a[contains(@class, 'member-listing__link')]"
# Listing label, e.g. "Member for Sandgate (ALP)".
TITLE = ".//span[contains(@class, 'member-listing__title')]"
TITLE_RE = re.compile(r"^Member for (?P<electorate>.+) \((?P<party>.+)\)$")
# A formal first name followed by a parenthetical preferred name, e.g.
# "Rosslyn (Ros) Bates" -> formal "Rosslyn Bates", preferred "Ros Bates".
NICKNAME_RE = re.compile(r"^(?P<first>\S+)\s*\((?P<nick>[^)]+)\)\s*(?P<rest>.*)$")
# Courtesy styles / titles prefixed to the display name; not part of the name.
HONORIFICS = {"Mr", "Mrs", "Ms", "Miss", "Dr", "Hon"}


def strip_honorific(name: str) -> str:
    """Drop a leading courtesy style ("Hon", "Mr", "Dr", …); keep the rest verbatim."""
    head, _, rest = name.partition(" ")
    if head in HONORIFICS:
        return rest.strip()
    return name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    card: Element,
) -> None:
    link = h.xpath_element(card, LINK)
    # The detail page is the canonical per-member URL; its opaque `id` query
    # parameter is the stable key we hash the entity ID from.
    detail_url = h.xpath_strings(link, "./@href")[0]
    raw_name = re.sub(
        r"\s+", " ", h.element_text(h.xpath_element(card, NAME)) or ""
    ).strip()

    title_text = None
    for title in h.xpath_elements(card, TITLE):
        text = h.element_text(title)
        if text is not None and text.startswith("Member for"):
            title_text = text
            break
    if title_text is None:
        context.log.warning("No member title", name=raw_name, url=detail_url)
        return
    match = TITLE_RE.match(title_text)
    if match is None:
        context.log.warning("Unparsable member title", title=title_text, url=detail_url)
        return
    electorate = match.group("electorate").strip()
    party = match.group("party").strip()

    no_honorific = strip_honorific(raw_name)
    nick_match = NICKNAME_RE.match(no_honorific)
    if nick_match is not None:
        full_name = f"{nick_match.group('first')} {nick_match.group('rest')}".strip()
        preferred = (
            f"{nick_match.group('nick').strip()} {nick_match.group('rest')}".strip()
        )
    else:
        full_name = no_honorific
        preferred = None

    person = context.make("Person")
    person.id = context.make_id(detail_url)
    h.apply_name(person, full=full_name, lang="eng")
    if preferred is not None:
        h.apply_name(person, full=preferred, lang="eng", alias=True)
    person.add("political", party)
    person.add("sourceUrl", detail_url)
    # Australian citizenship is a legal precondition to sit as a Queensland MLA:
    # Constitution of Queensland 2001 (Qld) s 21 — a candidate must be "an adult
    # Australian citizen living in Queensland".
    # https://www.legislation.qld.gov.au/view/whole/html/inforce/current/act-2001-080
    person.add("citizenship", "au")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", electorate)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Queensland",
        country="au",
        subnational_area="Queensland",
        wikidata_id="Q18526194",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    # The roster listing carries name, electorate and party for every member, so
    # no per-member detail fetch is needed. The only extra field on the detail
    # page is the election date (-> Occupancy startDate, and it distinguishes
    # by-election winners from the general-election cohort); fetch the detail page
    # per member if that is wanted in future.
    doc = context.fetch_html(MEMBERS_URL, absolute_links=True, cache_days=1)
    cards = h.xpath_elements(doc, CARD)
    if not (75 <= len(cards) <= 140):
        # 93 seats; a wild count means the listing layout changed.
        context.log.warning("Unexpected member count", count=len(cards))
    for card in cards:
        crawl_member(context, position, categorisation, card)
