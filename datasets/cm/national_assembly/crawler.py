from itertools import count

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

PAGE_SIZE = 15
MAX_PAGES = 20


def crawl_page(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    start: int,
) -> int:
    doc = context.fetch_html(context.data_url, params={"start": start}, cache_days=1)
    cards = h.xpath_elements(doc, '//div[@class="team-content"]')
    for card in cards:
        titles = h.xpath_elements(card, './/h3[@class="team-title"]')
        name = h.element_text(titles[0]) if titles else ""
        if not name:
            continue
        posts = h.xpath_elements(card, './/span[@class="post"]')
        party_region = h.element_text(posts[0]) if posts else ""
        # "PARTY-REGION", e.g. "UNDP-ADAMAWA"; the party abbreviation never contains a dash.
        party: str | None = None
        region: str | None = None
        if "-" in party_region:
            party, _, region = party_region.partition("-")
            party, region = party.strip() or None, region.strip() or None
        elif party_region:
            party = party_region

        person = context.make("Person")
        person.id = context.make_id(name, party_region)
        person.add("name", name)
        person.add("political", party)
        # National Assembly members must be Cameroonian citizens (Electoral Code,
        # Law No. 2012/001, Section 156). https://aceproject.org/electoral-advice/archive/questions/replies/7798903/986792279/ELECTORAL-CODE-OF-CAMEROON.pdf
        person.add("citizenship", "cm")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", region)
        context.emit(occupancy)
        context.emit(person)
    return len(cards)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Cameroon",
        country="cm",
        wikidata_id="Q21295975",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    total = 0
    for page in count(0):
        if page > MAX_PAGES:
            raise ValueError("National Assembly member list exceeded the page cap")
        count_on_page = crawl_page(context, position, categorisation, page * PAGE_SIZE)
        if count_on_page == 0:
            break
        total += count_on_page

    if total == 0:
        raise ValueError("No members found on the National Assembly pages")
