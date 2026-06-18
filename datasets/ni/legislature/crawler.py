from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


def crawl_deputy(
    context: Context,
    url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=7)

    # The detail page is an IBM XPages document; the deputy title span ("Diputado
    # Propietario" / "Diputada Propietaria") is immediately followed by the name span.
    title_el = h.xpath_element(doc, ".//span[contains(text(), 'Propietari')]")
    title = h.element_text(title_el)
    name = h.element_text(h.xpath_element(title_el, "./following-sibling::span[1]"))
    party = h.element_text(
        h.xpath_element(doc, ".//span[contains(@id, 'partidoPolitico1')]")
    )

    person = context.make("Person")
    person.id = context.make_slug(url.rsplit("documentId=", 1)[-1].split("&")[0])
    person.add("name", name)
    # The deputy title encodes gender
    person.add("gender", title)
    # Deputies must be Nicaraguan nationals (Political Constitution of Nicaragua,
    # Art. 134); nationality by birth is not specifically required.
    # https://pdba.georgetown.edu/Constitutions/Nica/nica95.html
    person.add("citizenship", "ni")
    # Party as published, e.g. "Alianza FSLN"; some deputies sit without one.
    person.add("political", party)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    anchors: list[Element] = h.xpath_elements(
        doc, ".//a[contains(@href, 'InfoDiputado.xsp')]"
    )
    # Deduplicate by document id; the listing may repeat links across view columns.
    urls: dict[str, str] = {}
    for anchor in anchors:
        href = anchor.get("href")
        if href is None:
            continue
        absolute = urljoin(context.data_url, href)
        doc_id = absolute.rsplit("documentId=", 1)[-1].split("&")[0]
        urls[doc_id] = absolute

    position = h.make_position(
        context,
        name="Member of the National Assembly of Nicaragua",
        country="ni",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18616113",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for url in urls.values():
        crawl_deputy(context, url, position, categorisation)
