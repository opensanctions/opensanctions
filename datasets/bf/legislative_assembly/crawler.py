from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise


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

    for card in h.xpath_elements(doc, '//div[contains(@class, "grid")]/div'):
        person = parse_person(
            context, h.element_text(h.xpath_element(card, ".//strong"))
        )
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
        context.emit(person)
