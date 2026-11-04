from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise

BUREAU_URL = "https://www.assembleenationale.bf/bure/depute"


def parse_person(context: Context, raw_name: str) -> Entity:
    """Build a deputy from a published `SURNAME Given Names` string."""
    name = h.strip_name_titles(context, raw_name)
    assert name is not None, raw_name
    last_name, _, first_name = name.partition(" ")
    # Every published name is one capitalised surname followed by given names. A
    # second capitalised word would be a multi-word surname, which this split would
    # silently mangle; initials such as "B." are not alphabetic and don't count.
    assert last_name.isupper() and len(first_name) > 0, raw_name
    assert not any(p.isupper() and p.isalpha() for p in first_name.split()), raw_name

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    # Deputies must hold Burkinabè nationality: Charte de la Transition of 14 October
    # 2022, Art. 12, renumbered Art. 13 by the revision of 25 May 2024.
    # https://mjp.univ-perp.fr/constit/bf2024-1.pdf
    person.add("citizenship", "bf")
    return person


def crawl_bureau(context: Context, members: dict[str, Entity]) -> None:
    """Add the presiding offices held by members of the Bureau."""
    doc = context.fetch_html(BUREAU_URL, cache_days=1)
    cards = h.xpath_elements(doc, '//div[strong and span[@class="text-md"]]')
    assert len(cards) > 0, BUREAU_URL
    for card in cards:
        raw_name = h.element_text(h.xpath_element(card, "./strong"))
        # Join on the cleaned name: the bureau page prefixes the President with "Dr ",
        # which the roster omits, so the raw names differ.
        name = h.strip_name_titles(context, raw_name)
        assert name is not None, raw_name
        person = members[name]
        role = h.element_text(h.xpath_element(card, "./span"))
        res = context.lookup("position", role)
        assert res is not None, role
        office = h.make_position(
            context,
            name=res.name,
            country="bf",
            topics=res.topics,
            wikidata_id=res.wikidata_id,
            lang="eng",
        )
        categorisation = categorise(context, office)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, office, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(office)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's Legislative Assembly of Burkina Faso",
        country="bf",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q141506877",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)

    # don't hard code parliamentary periods
    legislature = h.element_text(
        h.xpath_element(doc, '//p/a[starts-with(normalize-space(.), "Legislature")]')
    )
    period_start = legislature.split(":")[1].strip()
    assert period_start.isnumeric(), f"Period start should be year: {period_start}"

    members: dict[str, Entity] = {}
    for card in h.xpath_elements(doc, '//div[contains(@class, "grid")]/div'):
        raw_name = h.element_text(h.xpath_element(card, ".//strong"))
        person = parse_person(context, raw_name)
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            period_start=period_start,
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        name = h.strip_name_titles(context, raw_name)
        assert name is not None and name not in members, raw_name
        members[name] = person

    crawl_bureau(context, members)
    # Emit members last: the bureau occupancies also write to them.
    for person in members.values():
        context.emit(person)
