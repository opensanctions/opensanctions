from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


def crawl_member(
    context: Context,
    modal: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    member_id = modal.get("id")
    name = h.element_text(h.xpath_element(modal, ".//h4"))

    person = context.make("Person")
    person.id = context.make_id(member_id, name)
    h.apply_reviewed_name_string(
        context, person, string=name, lang="nld", llm_cleaning=True
    )
    # Members must hold Surinamese nationality (Constitution of Suriname, Art. 59).
    # https://www.constituteproject.org/constitution/Surinam_1992
    person.add("citizenship", "sr")

    party = h.element_text(
        h.xpath_element(
            modal, './/th[normalize-space()="Partij"]/following-sibling::td[1]'
        )
    )
    person.add("political", party)

    emails = h.xpath_strings(modal, './/a[starts-with(@href, "mailto:")]/@href')
    if emails != []:
        person.add("email", emails[0].removeprefix("mailto:").strip())

    data_url = modal.get("data-url")
    person.add("sourceUrl", urljoin(context.data_url, data_url))

    faction = h.element_text(
        h.xpath_element(
            modal, './/th[normalize-space()="Fractie"]/following-sibling::td[1]'
        )
    )
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", faction)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    # The roster page renders every member as a Bootstrap modal carrying their full
    # profile, so the whole dataset comes from a single page
    modals = h.xpath_elements(doc, './/div[contains(@class, "ledenModal")]')

    position = h.make_position(
        context,
        name="Member of the National Assembly of Suriname",
        country="sr",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q17268790",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for modal in modals:
        crawl_member(context, modal, position, categorisation)
