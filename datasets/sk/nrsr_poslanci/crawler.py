from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Slovak title of the position held by every member of the National Council.
POSITION_NAME = "Member of the National Council of the Slovak Republic"
POSITION_QID = "Q19967563"


def clean(value: str | None) -> str | None:
    """Strip non-breaking spaces and surrounding whitespace from a cell value."""
    if value is None:
        return None
    cleaned = value.replace("\xa0", " ").strip()
    return cleaned or None


def parse_details(doc: Element) -> dict[str, str | None]:
    """Build a label -> value mapping from the member detail page."""
    data: dict[str, str | None] = {}
    for block in h.xpath_elements(
        doc, '//div[@id="_sectionLayoutContainer__panelContent"]//div[strong]'
    ):
        label = clean(h.element_text(h.xpath_element(block, "./strong")))
        if label is None:
            continue
        full = h.element_text(block)
        value = clean(full[len(label) :]) if full.startswith(label) else clean(full)
        data[label] = value
    return data


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    href: str,
) -> None:
    doc = context.fetch_html(href, cache_days=7)
    data = parse_details(doc)

    first_name = data.pop("Meno", None)
    last_name = data.pop("Priezvisko", None)
    if first_name is None or last_name is None:
        context.log.warning("Member without a name", url=href)
        return

    person = context.make("Person")
    person.id = context.make_slug(href.split("PoslanecID=")[1].split("&")[0])
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="slk")
    person.add("title", data.pop("Titul", None))
    h.apply_date(person, "birthDate", data.pop("Narodený(á)", None))
    person.add("political", data.pop("Kandidoval(a) za", None), lang="slk")
    person.add("ethnicity", data.pop("Národnosť", None), lang="slk")
    emails = data.pop("E-mail", None)
    if emails is not None:
        person.add("email", h.multi_split(emails, [",", ";"]))
    person.add("website", data.pop("WWW", None))
    # Eligibility to the National Council requires Slovak citizenship per the
    # Constitution of the Slovak Republic, Art. 74(2):
    # https://www.nrsr.sk/web/Static/en-US/NRSR/Dokumenty/constitution.doc
    person.add("citizenship", "sk")
    person.add("sourceUrl", href)

    context.audit_data(data, ignore=["Bydlisko", "Kraj"])

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
    position = h.make_position(
        context,
        POSITION_NAME,
        country="sk",
        wikidata_id=POSITION_QID,
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    anchors = h.xpath_elements(
        doc,
        '//div[@id="_sectionLayoutContainer__panelContent"]'
        '//a[contains(@href, "PoslanecID")]',
    )
    if len(anchors) == 0:
        raise ValueError("No member links found on the list page")
    for anchor in anchors:
        href = anchor.get("href")
        if href is None:
            continue
        crawl_member(context, position, categorisation, href)
