from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h


def crawl_member(
    context: Context,
    card: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    titles = h.xpath_elements(card, './/h3[@class="team-title"]')
    name = h.element_text(titles[0])

    # The party/region label sits in a span.post, or - when that wrapper is
    # missing - directly in the second title heading.
    posts = h.xpath_elements(card, './/span[@class="post"]')
    party_region = h.element_text(posts[0]) if posts else h.element_text(titles[1])
    party, _, region = party_region.partition("-")

    person = context.make("Person")
    person.id = context.make_id(name, party_region)
    person.add("name", name)
    person.add("political", party.strip())
    # National Assembly members must be Cameroonian citizens (Electoral Code,
    # Law No. 2012/001, Section 156). https://aceproject.org/electoral-advice/archive/questions/replies/7798903/986792279/ELECTORAL-CODE-OF-CAMEROON.pdf
    person.add("citizenship", "cm")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", region.strip())
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Cameroon",
        country="cm",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295975",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Follow the "Next" pagination link until the last page, which has none.
    url: str | None = context.data_url
    while url is not None:
        doc = context.fetch_html(url, cache_days=30, absolute_links=True)
        for card in h.xpath_elements(doc, '//div[@class="team-content"]'):
            crawl_member(context, card, position, categorisation)
        next_urls = h.xpath_strings(
            doc, '//a[contains(@class, "page-link next")]/@href'
        )
        url = next_urls[0] if next_urls else None
