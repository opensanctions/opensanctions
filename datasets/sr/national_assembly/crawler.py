from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The roster page renders every member as a Bootstrap modal carrying their full
# profile, so the whole dataset comes from a single page — no per-member fetches.
MODAL_XPATH = './/div[contains(@class, "ledenModal")]'


def crawl_member(
    context: Context,
    modal: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # The modal id ("modalLid-10222") carries a stable numeric member id; the
    # data-url slug is name-derived and must not be used as an entity id.
    modal_id = modal.get("id")
    if modal_id is None or not modal_id.startswith("modalLid-"):
        raise ValueError(f"Unexpected member modal id: {modal_id!r}")
    member_id = modal_id.removeprefix("modalLid-")

    name = h.element_text(h.xpath_element(modal, ".//h4"))
    party = h.element_text(
        h.xpath_element(
            modal, './/th[normalize-space()="Partij"]/following-sibling::td[1]'
        )
    )
    fractie = h.element_text(
        h.xpath_element(
            modal, './/th[normalize-space()="Fractie"]/following-sibling::td[1]'
        )
    )
    emails = h.xpath_strings(modal, './/a[starts-with(@href, "mailto:")]/@href')
    data_url = modal.get("data-url")

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    # Names carry Dutch academic honorifics (dr., drs., mr., ir.) and degree suffixes
    # (MSc, LLB, PhD, ...) mixed in; defer cleaning to the name-review workflow, which
    # applies the original string until a reviewed split is accepted.
    h.apply_reviewed_name_string(
        context, person, string=name, lang="nld", llm_cleaning=True
    )
    # Members must hold Surinamese nationality (Constitution of Suriname, Art. 59).
    # https://www.constituteproject.org/constitution/Surinam_1992
    person.add("citizenship", "sr")
    person.add("political", party)
    if len(emails) > 0:
        person.add("email", emails[0].removeprefix("mailto:"))
    if data_url is not None:
        person.add("sourceUrl", urljoin(context.data_url, data_url))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # The fractie (parliamentary group) is distinct from party membership.
    occupancy.add("politicalGroup", fractie)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    modals = h.xpath_elements(doc, MODAL_XPATH)
    if len(modals) == 0:
        raise ValueError("No member modals found on the roster page")

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
