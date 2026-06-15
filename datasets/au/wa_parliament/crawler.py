import re
from urllib.parse import unquote_plus, urlparse

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract import zyte_api
from zavod.util import Element

MEMBERS_URL = "https://www.parliament.wa.gov.au/parliament/memblist.nsf/WAllMembers"
# All 96 members (both houses) are listed in one table; each Member cell is an
# anchor into the WAllMembersFlat2 Domino view, followed by a "MLA"/"MLC" suffix
# and a "Party: <abbr>" line.
MEMBER_LINKS = '//a[contains(@href, "WAllMembersFlat2")]'

# The "MLA"/"MLC" suffix after the member anchor selects the chamber/position.
CHAMBERS: dict[str, dict[str, str]] = {
    "MLA": {
        "name": "Member of the Western Australian Legislative Assembly",
        "wikidata_id": "Q20165902",
    },
    "MLC": {
        "name": "Member of the Western Australian Legislative Council",
        "wikidata_id": "Q19627913",
    },
}

# The chamber token sits in the anchor's tail, sometimes after a post-nominal
# (e.g. "CSC, MLA"), so match it rather than comparing the whole tail.
SUFFIX_RE = re.compile(r"\b(MLA|MLC)\b")
PARTY_RE = re.compile(r"Party:\s*(.+)$")
NICKNAME_RE = re.compile(r"\(([^)]+)\)")


def parse_name(profile_url: str) -> tuple[str, str, str | None]:
    """Return (first_name, last_name, preferred_name) from the URL key.

    The Domino profile-link key is "Surname,+Firstname", sometimes with a
    preferred name in parentheses ("Kent,+Alison+(Ali)"). We read the name from
    the key because it is properly cased, unlike the visible anchor which
    uppercases the surname. The key is the *display* name only; the entity ID
    keys on the raw URL instead (see crawl_member).
    """
    key = unquote_plus(urlparse(profile_url).path.rstrip("/").rsplit("/", 1)[-1])
    last_name, _, first_field = key.partition(",")
    first_field = first_field.strip()
    nick_match = NICKNAME_RE.search(first_field)
    preferred = nick_match.group(1).strip() if nick_match else None
    first_name = (h.remove_bracketed(first_field) or "").strip()
    return first_name, last_name.strip(), preferred


def crawl_member(
    context: Context,
    chambers: dict[str, tuple[Entity, PositionCategorisation]],
    anchor: Element,
) -> None:
    profile_url = h.xpath_strings(anchor, "./@href")[0]
    # The chamber token ("MLA" / "MLC") follows the member anchor.
    suffix_match = SUFFIX_RE.search(anchor.tail or "")
    if suffix_match is None:
        context.log.warning("No chamber suffix", tail=anchor.tail, url=profile_url)
        return
    position, categorisation = chambers[suffix_match.group(1)]

    # Row layout: Photo | Member | Electorate | Contact.
    row_cells = h.xpath_elements(anchor, "./ancestor::tr[1]/td")
    party_match = PARTY_RE.search(h.element_text(row_cells[1]))
    party = party_match.group(1).strip() if party_match else None
    electorate = h.element_text(row_cells[2]) or None

    first_name, last_name, preferred = parse_name(profile_url)
    person = context.make("Person")
    # Key on the raw profile URL, not the cleaned name.
    person.id = context.make_id(profile_url)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="eng")
    if preferred is not None and preferred != first_name:
        h.apply_name(
            person, first_name=preferred, last_name=last_name, lang="eng", alias=True
        )
    person.add("political", party)
    person.add("sourceUrl", profile_url)
    # Australian citizenship is a legal precondition for WA members: enrolment
    # requires Australian citizenship (Electoral Act 1907 (WA) s 17(1)(a)), and
    # candidate eligibility is tied to enrolment (s 76A).
    # https://www.legislation.wa.gov.au/legislation/statutes.nsf/law_a247.html
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
    chambers: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for suffix, config in CHAMBERS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="au",
            subnational_area="Western Australia",
            wikidata_id=config["wikidata_id"],
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        chambers[suffix] = (position, categorisation)

    doc = zyte_api.fetch_html(
        context,
        MEMBERS_URL,
        unblock_validator=MEMBER_LINKS,
        absolute_links=True,
        cache_days=1,
    )
    anchors = h.xpath_elements(doc, MEMBER_LINKS)
    if not (80 <= len(anchors) <= 115):
        # 96 members (59 + 37); a wild count means the listing layout changed.
        context.log.warning("Unexpected member count", count=len(anchors))
    for anchor in anchors:
        crawl_member(context, chambers, anchor)
