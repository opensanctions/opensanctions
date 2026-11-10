import re

from lxml.html import HtmlElement
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context
from zavod import helpers as h

CONGRESS_RE = re.compile(r"\b(\d+)(st|nd|rd|th) Congress\b")
LEGISLATOR_ID_RE = re.compile(r"^[A-Z0-9]+$")
# data-status on each card. The page's member count badge only includes "1", and
# the others carry labels such as "Resigned" on their detail pages.
STATUS_SEATED = "1"
STATUS_UNSEATED = {"3", "4", "5"}


def crawl_card(
    context: Context,
    card: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = h.element_text(
        h.xpath_element(card, ".//h6[contains(@class, 'hm-fullname')]")
    )
    status = card.get("data-status")
    if status in STATUS_UNSEATED:
        context.log.info("Skipping member no longer seated", name=name, status=status)
        return
    assert status == STATUS_SEATED, f"Unknown member status {status!r} for {name!r}"

    # Member codes are permanent legislator IDs: the letter marks the Congress in
    # which they were first seated (e.g. L001 for the 20th), carried across terms.
    url = h.xpath_string(card, ".//a[h6[contains(@class, 'hm-fullname')]]/@href")
    code = url.rstrip("/").rsplit("/view/", 1)[-1]
    if not LEGISLATOR_ID_RE.match(code):
        raise ValueError(f"Unexpected profile URL for {name!r}: {url!r}")

    # e.g. ["District Representative", "Kalinga, Lone District"] or
    # ["Party-list Representative", "4Ps"]
    seat_texts = h.xpath_strings(
        card, ".//small[contains(@class, 'text-muted')]//text()"
    )
    seat = [t.strip() for t in seat_texts if t.strip()]
    assert len(seat) == 2, f"Unexpected seat description for {name!r}: {seat!r}"
    member_type, seat_name = seat

    person = context.make("Person")
    person.id = context.make_slug(code)
    # Names are inverted with nicknames and degrees, e.g. "Agyao, Caroline B., M.D."
    # or "Dy, Faustino "Bojie" III G.". The degrees are a fixed set stripped via the
    # metadata; nickname categorisation goes through name review.
    h.apply_reviewed_name_string(
        context,
        person,
        string=h.strip_name_titles(context, name),
        llm_cleaning=True,
        lang="eng",
    )
    person.add("sourceUrl", url)

    constituency: str | None = None
    if member_type == "Party-list Representative":
        # Party-list reps represent a national/sectoral organisation rather than a
        # territorial district; that organisation is their political vehicle.
        person.add("political", seat_name, lang="eng")
    elif member_type == "District Representative":
        area, _, district = seat_name.rpartition(", ")
        if not area or not district.endswith("District"):
            raise ValueError(f"Unexpected district for {name!r}: {seat_name!r}")
        constituency = f"{district}, {area}"
    else:
        raise ValueError(f"Unknown member type for {name!r}: {member_type!r}")

    # Membership of the House requires natural-born Philippine citizenship for both
    # district and party-list representatives: 1987 Constitution, Article VI, Section 6.
    # https://www.officialgazette.gov.ph/constitutions/1987-constitution/
    person.add("citizenship", "ph")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(person)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of the Philippines",
        country="ph",
        topics=["gov.legislative", "gov.national"],
        wikidata_id="Q18002923",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    cards_xpath = "//div[@id='galleryCards']//div[contains(@class, 'member-card')]"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        cards_xpath,
        geolocation="PH",
        absolute_links=True,
    )

    for card in h.xpath_elements(doc, cards_xpath):
        crawl_card(context, card, position, categorisation)

    assert_all_accepted(context, raise_on_unaccepted=False)
