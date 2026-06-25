from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Côte d'Ivoire",
        country="ci",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    # Senators are rendered as cards; the same senator recurs under each commission
    # section they sit on, so dedupe by name.
    cards = h.xpath_elements(doc, "//div[contains(@class, 'tof-card')]")
    seen: set[str] = set()
    for card in cards:
        titles = h.xpath_elements(card, ".//h6[contains(@class, 'card-title')]")
        if not titles:
            continue
        name = h.element_text(titles[0])
        if len(name) == 0 or name in seen:
            continue
        seen.add(name)

        person = context.make("Person")
        person.id = context.make_id("ci-senator", name)
        person.add("name", name)
        # Senators must be Ivorian nationals (Constitution art. 87; Electoral Code
        # art. 112). https://www.constituteproject.org/constitution/Cote_DIvoire_2016
        person.add("citizenship", "ci")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)

    if len(seen) == 0:
        raise ValueError("No senators found on the listing page")
