from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Senator of Cameroon",
        country="cm",
        wikidata_id="Q21295128",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    cards = h.xpath_elements(
        doc, '//div[contains(@class, "sptp-member") and contains(@class, "border-bg")]'
    )
    if not cards:
        raise ValueError("No senator cards found on the Senate page")

    seen: set[str] = set()
    for card in cards:
        names = h.xpath_elements(card, './/div[@class="sptp-member-name"]')
        name = h.element_text(names[0]) if names else ""
        if not name or name in seen:
            continue
        seen.add(name)
        roles = h.xpath_elements(card, './/div[@class="sptp-member-profession"]')
        role = h.element_text(roles[0]) if roles else None

        person = context.make("Person")
        person.id = context.make_id(name)
        person.add("name", name, lang="fra")
        # Senators must be Cameroonian citizens (Electoral Code, Law No. 2012/001,
        # Section 220(2)). https://aceproject.org/electoral-advice/archive/questions/replies/7798903/986792279/ELECTORAL-CODE-OF-CAMEROON.pdf
        person.add("citizenship", "cm")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        if role:
            occupancy.add("description", role, lang="fra")
        context.emit(occupancy)
        context.emit(person)
