"""Crawler for the members of the Egyptian House of Representatives.

The parliament's website exposes one profile page per member, addressed by a
sequential ``id`` query parameter (``MembersDetails.aspx?id=N``). There is no
list page linking the profiles, so members are discovered by enumerating ``id``
from 1 upwards until a run of empty pages indicates the end of the roster.
"""

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The profile <span> element ids are stable across the site (ASP.NET WebForms
# control ids). An out-of-range or vacated id yields a non-member page: the
# profile spans are either rendered empty or omitted from the markup entirely,
# both of which the enumeration treats as "no member here".
NAME_XPATH = "//span[@id='ContentPlaceHolder1_Label2']"
BIRTH_PLACE_XPATH = "//span[@id='ContentPlaceHolder1_Label4']"
PARTY_XPATH = "//span[@id='ContentPlaceHolder1_Label8']"

# Stop enumerating after this many consecutive empty pages. Comfortably larger
# than any plausible gap in the id sequence, so interior gaps don't end the crawl
# early, while a redesigned site (all pages empty) still terminates.
MAX_CONSECUTIVE_EMPTY = 25
# Hard upper bound on ids to probe, as a runaway guard.
MAX_MEMBER_ID = 1000


def field(doc: Element, xpath: str) -> str | None:
    """Return the stripped text of the element at ``xpath``, or None.

    Non-member pages may omit the profile spans entirely rather than render
    them empty, so a zero-match result is a valid "no data here" signal and
    yields None. More than one match is unexpected and still raises.
    """
    elements = h.xpath_elements(doc, xpath)
    if len(elements) == 0:
        return None
    if len(elements) > 1:
        raise ValueError(
            f"Expected at most one element for xpath {xpath!r}, got {len(elements)}"
        )
    text = h.element_text(elements[0])
    return text or None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    doc: Element,
) -> bool:
    """Emit one member profile. Returns True if the page held a member."""
    name = field(doc, NAME_XPATH)
    if name is None:
        # Empty span: this id does not correspond to a seated member.
        return False

    birth_place = field(doc, BIRTH_PLACE_XPATH)

    person = context.make("Person")
    # The source id is reused across parliamentary terms (id 1 refers to a
    # different person each term), so anchor identity on the person, not the id.
    person.id = context.make_id(name, birth_place)
    person.add("name", name)  # Arabic; data.lang default applies
    person.add("birthPlace", birth_place)
    person.add("political", field(doc, PARTY_XPATH))
    # Membership of the House legally requires Egyptian citizenship: Constitution
    # of Egypt (2014, as amended 2019), Article 102.
    # https://www.constituteproject.org/constitution/Egypt_2019
    person.add("citizenship", "eg")

    # make_occupancy reads person props to decide PEP status, so all person
    # properties must be set before this call.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # Official live roster with no per-member dates: no end date means the
        # member currently holds the seat.
        no_end_implies_current=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)
    return True


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Egypt",
        country="eg",
        wikidata_id="Q21290857",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    # Gate even with default_is_pep=True: the position may have been un-flagged
    # in the review UI, in which case emit nothing.
    if not categorisation.is_pep:
        return
    context.emit(position)

    member_count = 0
    consecutive_empty = 0
    member_id = 1
    while member_id <= MAX_MEMBER_ID and consecutive_empty < MAX_CONSECUTIVE_EMPTY:
        url = f"{context.data_url}?id={member_id}"
        doc = context.fetch_html(url, cache_days=1)
        if crawl_member(context, position, categorisation, doc):
            member_count += 1
            consecutive_empty = 0
        else:
            consecutive_empty += 1
        member_id += 1

    context.log.info("Crawled House of Representatives members", count=member_count)
    if member_count == 0:
        raise RuntimeError(
            "No members found; the source page structure may have changed."
        )
