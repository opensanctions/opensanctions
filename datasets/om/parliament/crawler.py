import re
from typing import NamedTuple

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


# Position kinds. The chambers label the rank and file inconsistently ("Member" /
# "member" / "Member of State Council") and use different leadership titles (Chairman
# vs Chairperson, Deputy Chairman vs Vice Chairperson), so listing roles are classified
# by keyword into one of these kinds. Each kind maps to the prefix of its position name.
MEMBER = "member"
CHAIRMAN = "chairman"
DEPUTY = "deputy"
POSITION_PREFIX = {
    MEMBER: "Member",
    CHAIRMAN: "Chairman",
    DEPUTY: "Deputy Chairman",
}


class Chamber(NamedTuple):
    slug: str
    listing_url: str
    # English name of the legislative body, woven into every position name.
    body: str
    # Wikidata position items keyed by kind. A kind absent here gets a name-derived id
    # because no settled Wikidata item exists for it (the deputy/vice-chair roles and
    # the State Council chair).
    position_qids: dict[str, str]


# Both chambers of Majlis Oman run the same Kentico WebForms portal: an identical
# member-listing page, pager and period filter. Only the host, the body name and the
# available Wikidata positions differ.
CHAMBERS = [
    Chamber(
        "shura",
        "https://www.shura.om/Member/Members",
        "Consultative Assembly of Oman",
        {MEMBER: "Q21328587", CHAIRMAN: "Q114590675"},
    ),
    Chamber(
        "statecouncil",
        "https://www.statecouncil.om/Member/Members",
        "State Council of Oman",
        {MEMBER: "Q21328599"},
    ),
]

# The portal serves Arabic unless the request asks for the English (en-GB) culture.
ACCEPT_LANGUAGE = "en-GB,en;q=0.9"

# WebForms control that drives listing pagination via __doPostBack. Identical on both
# portals.
PAGER_TARGET = "p$lt$ctl09$wP$p$lt$ctl00$wU$pagerElem"
PAGER_ARG_RE = re.compile(r"pagerElem'\s*,\s*'(\d+)'")

# A legislative term as printed in the period filter, e.g. "2023-2027".
TERM_RE = re.compile(r"^\s*(\d{4})-(\d{4})\s*$")


class Term(NamedTuple):
    value: str  # the period filter's <option> value used to select the term
    start: str  # start year, e.g. "2023"
    end: str  # end year, e.g. "2027"


class Card(NamedTuple):
    member_id: str
    name: str
    role: str


class Member(NamedTuple):
    member_id: str
    name: str
    kinds: set[str]


def classify_role(role: str) -> str:
    """Map a listing role label to a position kind (member / chairman / deputy).

    The card's role line is overloaded: it carries a leadership title ("Chairman",
    "Vice Chairperson", "Deputy chairman Majlis bureau", ...) where there is one, and
    otherwise the member's electoral district — sometimes in Arabic. Leadership is
    therefore matched by keyword and everything else is treated as a plain member.
    """
    text = role.casefold()
    if "deputy" in text or "vice" in text:
        return DEPUTY
    if "chair" in text:  # "Chairman", "Chairperson"
        return CHAIRMAN
    return MEMBER


def request_headers(chamber: Chamber) -> dict[str, str]:
    """Headers every request needs: the English culture and a member-area referer.

    Without the ``Referer`` the portal redirects member-area requests back to the home
    page; without ``Accept-Language`` it serves Arabic.
    """
    return {"Accept-Language": ACCEPT_LANGUAGE, "Referer": chamber.listing_url}


def hidden_fields(doc: HtmlElement) -> dict[str, str]:
    """Collect the WebForms hidden form fields (__VIEWSTATE et al.) from a page.

    These must be echoed back verbatim in each postback, so they are parsed from one
    response and reused for the postbacks driven off it.
    """
    fields: dict[str, str] = {}
    for el in h.xpath_elements(doc, '//input[@type="hidden"]'):
        name = el.get("name")
        if name is not None:
            fields[name] = el.get("value") or ""
    return fields


def read_terms(doc: HtmlElement) -> tuple[str, list[Term]]:
    """Return the period-filter control name and the legislative terms it offers.

    The terms come from the page rather than being hardcoded, so a newly seated term
    is crawled automatically and the option values (which differ between chambers) stay
    correct. A filter without any ``YYYY-YYYY`` option fails loudly.
    """
    select = h.xpath_element(
        doc,
        '//select[contains(@name,"MemberFilterAdvance")]'
        '[contains(@name,"CategoryTerms")]',
    )
    name = select.get("name")
    assert name is not None
    terms: list[Term] = []
    for option in h.xpath_elements(select, "./option"):
        match = TERM_RE.match(h.element_text(option))
        value = option.get("value")
        if match is not None and value is not None:
            terms.append(Term(value, match.group(1), match.group(2)))
    if len(terms) == 0:
        raise ValueError("No legislative terms found in the period filter")
    return name, terms


def last_page(doc: HtmlElement) -> int:
    """The highest page number offered by the pager, or 1 if there is no pager."""
    args = [
        int(m.group(1))
        for href in h.xpath_strings(doc, '//a[contains(@href,"pagerElem")]/@href')
        if (m := PAGER_ARG_RE.search(href)) is not None
    ]
    return max(args) if len(args) else 1


def parse_cards(context: Context, doc: HtmlElement) -> list[Card]:
    """Extract the member cards (id, name, role) from a listing page."""
    cards: list[Card] = []
    for el in h.xpath_elements(doc, '//div[contains(@class,"innermembers")]'):
        hrefs = h.xpath_strings(el, './/a[contains(@href,"MembersDetails")]/@href')
        ids = {m.group(1) for href in hrefs if (m := re.search(r"id=(\d+)", href))}
        if len(ids) != 1:
            context.log.warning("Member card without a single id", hrefs=hrefs)
            continue
        name = h.element_text(h.xpath_element(el, ".//h1"))
        role = h.element_text(h.xpath_element(el, ".//p"))
        if name == "" or role == "":
            context.log.warning("Member card missing name or role", id=ids)
            continue
        cards.append(Card(ids.pop(), name, role))
    return cards


def fetch_term(
    context: Context, chamber: Chamber, select_name: str, term: Term
) -> list[Card]:
    """Select one term in the period filter and page through its member listing.

    A fresh GET seeds the hidden form fields; selecting the term is one postback, and
    each further page replays the pager postback with the term still selected.
    """
    headers = request_headers(chamber)
    base = context.fetch_html(chamber.listing_url, headers=headers, cache_days=1)

    select_data = hidden_fields(base)
    select_data[select_name] = term.value
    select_data["__EVENTTARGET"] = select_name
    select_data["__EVENTARGUMENT"] = ""
    page = context.fetch_html(
        chamber.listing_url,
        method="POST",
        data=select_data,
        headers=headers,
        cache_days=1,
    )

    fields = hidden_fields(page)
    fields[select_name] = term.value
    cards: dict[str, Card] = {c.member_id: c for c in parse_cards(context, page)}
    for number in range(2, last_page(page) + 1):
        data = dict(fields)
        data["__EVENTTARGET"] = PAGER_TARGET
        data["__EVENTARGUMENT"] = str(number)
        doc = context.fetch_html(
            chamber.listing_url,
            method="POST",
            data=data,
            headers=headers,
            cache_days=1,
        )
        for card in parse_cards(context, doc):
            cards[card.member_id] = card
    return list(cards.values())


def name_key(name: str) -> str:
    """Normalise a name for matching the leadership and member cards of one person."""
    return " ".join(name.split()).casefold()


def group_members(cards: list[Card]) -> list[Member]:
    """Merge the cards of each person within a term and resolve their position kinds.

    A member who also holds a leadership office appears under two distinct source ids
    (a leadership card and a plain member card) with the same name. They are merged on
    the normalised name; the plain member card supplies the person's id when present,
    otherwise the (single) leadership card does. Merging is only ever within a term,
    where the source spells a name consistently — members are not merged across terms.
    """
    merged: dict[str, Member] = {}
    for card in cards:
        kind = classify_role(card.role)
        key = name_key(card.name)
        member = merged.get(key)
        if member is None:
            member = Member(card.member_id, card.name, set())
            merged[key] = member
        if kind == MEMBER:
            merged[key] = member._replace(member_id=card.member_id, name=card.name)
        else:
            member.kinds.add(kind)
    return list(merged.values())


def get_position(
    context: Context,
    chamber: Chamber,
    kind: str,
    cache: dict[str, tuple[Entity, PositionCategorisation]],
) -> tuple[Entity, PositionCategorisation]:
    """Return the position for a chamber-and-kind, building it on first use.

    Positions are created lazily so a chamber only emits the leadership positions it
    actually fills (the State Council, for instance, lists no chairman). The cache is
    shared across terms, so every term's occupancies hang off one position per kind.
    """
    if kind not in cache:
        name = f"{POSITION_PREFIX[kind]} of the {chamber.body}"
        position = h.make_position(
            context,
            name=name,
            country="om",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=chamber.position_qids.get(kind),
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        cache[kind] = (position, categorisation)
    return cache[kind]


def crawl_member(
    context: Context,
    chamber: Chamber,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    member: Member,
    term: Term,
) -> None:
    """Emit one member with a membership occupancy plus any leadership occupancies."""
    person = context.make("Person")
    person.id = context.make_slug(chamber.slug, member.member_id)
    h.apply_name(person, full=member.name)

    # commenting out the sourceUrl as it doesn't work on its own, but
    # requires a cookie state or referer to be set, otherwise it redirects
    # to the home page; maybe it will work in the future
    # detail = f"{chamber.listing_url}/MembersDetails?id={member.member_id}"
    # person.add("sourceUrl", detail)

    # Membership of Majlis Oman is reserved to Omani nationals.
    person.add("citizenship", "om")

    emitted = False
    for kind in (MEMBER, *sorted(member.kinds)):
        position, categorisation = get_position(context, chamber, kind, positions)
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=term.start,
            end_date=term.end,
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy)
            emitted = True
    if emitted:
        context.emit(person)


def crawl_chamber(context: Context, chamber: Chamber) -> None:
    base = context.fetch_html(
        chamber.listing_url, headers=request_headers(chamber), cache_days=1
    )
    select_name, terms = read_terms(base)
    # One position cache per chamber: occupancies from every term share a position.
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for term in terms:
        members = group_members(fetch_term(context, chamber, select_name, term))
        context.log.info(
            "Fetched term",
            chamber=chamber.slug,
            term=f"{term.start}-{term.end}",
            members=len(members),
        )
        context.log.info(
            "members by term",
            chamber=chamber.slug,
            term=f"{term.start}-{term.end}",
            member_count=len(members),
        )
        for member in members:
            crawl_member(context, chamber, positions, member, term)


def crawl(context: Context) -> None:
    for chamber in CHAMBERS:
        crawl_chamber(context, chamber)
