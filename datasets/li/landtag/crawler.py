from datetime import datetime
from urllib.parse import urlparse

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

MEMBER_NAME = "Member of the Landtag of Liechtenstein"
SUBSTITUTE_NAME = "Substitute member of the Landtag of Liechtenstein"

# The roster's "Legislaturperiode" dropdown carries one option per legislative term,
# its value being the term's start date (e.g. "25.03.2021") and the path it links to
# being /abgeordnete/<year>. We iterate those pages so that members of past terms (no
# longer in office) are retained, bounded by earliest_term_start so we don't reach
# further back than positions are still considered PEP-relevant.
PERIOD_VALUE_FORMAT = "%d.%m.%Y"


def make_member_position(context: Context, substitute: bool) -> Entity:
    return h.make_position(
        context,
        name=SUBSTITUTE_NAME if substitute else MEMBER_NAME,
        country="li",
        topics=["gov.national", "gov.legislative"],
        # Only the full member position has a dedicated Wikidata item.
        wikidata_id=None if substitute else "Q21328607",
    )


def parse_terms(index: Element) -> list[tuple[str, str, str | None]]:
    """Return the legislative terms to crawl as ``(year, period_start, period_end)``.

    Terms are read newest-first from the period dropdown. ``period_start`` is the term's
    own start date; ``period_end`` is the start date of the next (newer) term, i.e. when
    this term ended — ``None`` for the current term. Terms whose end falls before
    ``earliest_term_start`` are dropped, so the reach into the past tracks the PEP
    after-office window rather than a hard-coded year.
    """
    cutoff = h.earliest_term_start(["gov.national"])
    options = h.xpath_elements(index, '//select[@id="main_ddYears"]/option')
    starts = [h.xpath_strings(o, "./@value", expect_exactly=1)[0] for o in options]

    terms: list[tuple[str, str, str | None]] = []
    for i, start in enumerate(starts):
        # The dropdown JS navigates to /abgeordnete/<year> using the value's year part.
        year = start.split(".")[-1]
        period_end = starts[i - 1] if i > 0 else None
        if period_end is not None:
            end_iso = datetime.strptime(period_end, PERIOD_VALUE_FORMAT).date()
            if end_iso.isoformat() < cutoff:
                continue
        terms.append((year, start, period_end))
    return terms


def crawl_detail(context: Context, person: Entity, url: str) -> None:
    """Set the fields only carried on the member detail page: birth date and profession.

    Date of birth is given either as a full ``Geburtsdatum`` (DD.MM.YYYY) or, for members
    who only disclose the year, as ``Jahrgang`` (YYYY). We deliberately ignore the other
    detail-page fields (previous functions, e-mail, marital status, residence).
    """
    doc = context.fetch_html(url, cache_days=6)
    # Scope to the "Persönliche Informationen" table (table--attr); the page also has a
    # "Bisherige Funktionen" table (table--period) whose roles we deliberately skip.
    info_table = h.xpath_element(doc, '//table[contains(@class,"table--attr")]')
    bio: dict[str, str] = {}
    for row in h.xpath_elements(info_table, ".//tr"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) != 2:
            continue
        bio[h.element_text(cells[0])] = h.element_text(cells[1])

    h.apply_date(
        person, "birthDate", bio.pop("Geburtsdatum", None) or bio.pop("Jahrgang", None)
    )
    person.add("profession", bio.pop("Beruf", None))
    context.audit_data(bio, ignore=["Partei", "Wohnort", "Familienstand", "E-Mail"])


def get_person(context: Context, cache: dict[str, Entity], card: Element) -> Entity:
    """Build (once per source id) the Person for a roster card.

    A member recurs across the term pages we crawl; we key on the source's person id so
    the detail page is fetched only once and the per-term occupancies all attach to the
    same entity.
    """
    link = h.xpath_strings(
        card, './/div[contains(@class,"member__name")]/a/@href', expect_exactly=1
    )[0]
    source_id = urlparse(link).path.rstrip("/").split("/")[-1]
    if source_id in cache:
        return cache[source_id]

    # The roster lists members surname-first, e.g. "Bühler-Nigsch Dagmar". We keep the
    # full string as the name regardless, and only split into surname/given name in the
    # unambiguous single-space case — multi-token names are left unsplit rather than
    # guessed at.
    name = h.element_text(
        h.xpath_element(card, './/*[contains(@class,"member__name2")]')
    )
    first_name = last_name = None
    if name.count(" ") == 1:
        last_name, first_name = name.split(" ")

    person = context.make("Person")
    person.id = context.make_slug("person", source_id)
    h.apply_name(person, full=name, first_name=first_name, last_name=last_name)
    # Standing for the Landtag requires Liechtenstein citizenship (political rights in
    # national matters): Verfassung des Fürstentums Liechtenstein, Art. 29(2).
    # https://www.gesetze.li/konso/pdf/1921.015
    person.add("citizenship", "li")
    party = h.xpath_element(card, './/*[contains(@class,"member__party")]')
    person.add("political", h.element_text(party))
    crawl_detail(context, person, link)

    cache[source_id] = person
    return person


def crawl_term(
    context: Context,
    year: str,
    period_start: str,
    period_end: str | None,
    positions: dict[bool, Entity],
    cats: dict[bool, PositionCategorisation],
    persons: dict[str, Entity],
    emitted: set[str],
) -> None:
    doc = context.fetch_html(
        f"{context.data_url}/{year}", absolute_links=True, cache_days=1
    )
    for card in h.xpath_elements(doc, '//div[@class="abglist__member member"]'):
        card_id = card.get("id") or ""
        # The leadership highlight block ("Leaders") duplicates members listed below.
        if "Leaders" in card_id:
            continue
        # Substitutes ("Deputy"); full members are in the Members and Resigned
        # ("Ausgeschieden", i.e. left mid-term) sections.
        is_substitute = "Deputy" in card_id
        person = get_person(context, persons, card)
        assert person.id is not None
        occupancy = h.make_occupancy(
            context,
            person,
            positions[is_substitute],
            categorisation=cats[is_substitute],
            period_start=period_start,
            period_end=period_end,
        )
        if occupancy is not None:
            context.emit(occupancy)
            emitted.add(person.id)


def crawl(context: Context) -> None:
    index = context.fetch_html(context.data_url, cache_days=1)
    terms = parse_terms(index)

    positions = {
        False: make_member_position(context, substitute=False),
        True: make_member_position(context, substitute=True),
    }
    cats = {}
    for substitute, position in positions.items():
        context.emit(position)
        cats[substitute] = categorise(context, position, default_is_pep=True)

    persons: dict[str, Entity] = {}
    emitted: set[str] = set()
    for year, period_start, period_end in terms:
        crawl_term(
            context, year, period_start, period_end, positions, cats, persons, emitted
        )

    if not emitted:
        raise ValueError("No members found across any term; the roster layout changed.")
    for source_id, person in persons.items():
        if person.id in emitted:
            context.emit(person)
