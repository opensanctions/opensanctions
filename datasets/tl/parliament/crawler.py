import re

from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

PROFILE_URL = "https://www.parlamento.tl/deputados/"
# Each party's members are listed in a modal headed e.g. "CNRT - Deputados".
HEADING_SUFFIX = " - Deputados"
# Nicknames are quoted inline, e.g. 'Maria Rosa da Câmara "Bisoi"'.
NICKNAME_RE = re.compile(r'\s*"([^"]+)"')


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    link: Element,
    party: str,
) -> None:
    url = h.xpath_string(link, "./@href")
    raw_name = h.element_text(link)

    person = context.make("Person")
    person.id = context.make_slug(raw_name)
    person.add("name", NICKNAME_RE.sub("", raw_name), lang="por")
    person.add("alias", NICKNAME_RE.findall(raw_name))
    person.add("political", party)
    # Every citizen over seventeen has the right to be elected (Constitution of the
    # RDTL, Section 47(1)). https://www.constituteproject.org/constitution/East_Timor_2002
    person.add("citizenship", "tl")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Parliament of Timor-Leste",
        country="tl",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q19966812",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    for modal in h.xpath_elements(doc, '//div[starts-with(@id, "modal-")]'):
        heading = h.xpath_string(
            modal, f'.//h3[contains(text(), "{HEADING_SUFFIX}")]/text()'
        )
        party = heading.strip().removesuffix(HEADING_SUFFIX)
        links = h.xpath_elements(modal, f'.//a[starts-with(@href, "{PROFILE_URL}")]')
        for link in links:
            crawl_member(context, position, categorisation, link, party)
