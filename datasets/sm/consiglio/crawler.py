from normality import squash_spaces

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


def crawl_member(
    context: Context,
    member: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    heading = h.xpath_element(member, ".//h3")
    name = h.element_text(heading)
    # The roster carries an empty placeholder block with no name or party link; skip it.
    if not name:
        return
    party = squash_spaces(h.xpath_string(member, "./a/text()"))

    person = context.make("Person")
    person.id = context.make_id(name, party)
    person.add("name", name)
    person.add("political", party, lang="ita")
    # Standing for the Council requires Sammarinese citizenship: candidates must meet
    # the elector conditions of the Electoral Law (Art. 4, native/naturalised
    # Sammarinese) plus Art. 21. https://www.consigliograndeegenerale.sm/on-line/home/composizione/elenco-consiglieri.html
    person.add("citizenship", "sm")

    # The councillor's name links to a detail page that records their date of birth.
    detail_url = h.xpath_string(heading, ".//a/@href")
    person.add("sourceUrl", detail_url)

    detail = context.fetch_html(detail_url, encoding="latin-1", cache_days=1)
    # Detail fields are "<span>label:</span> value" rows; the DOB is Italian
    # "<day> <month name> <year>", normalised via the dates config.
    dob = h.xpath_string(
        detail,
        '//div[@class="campodettaglio"]'
        '[span[normalize-space()="data nascita:"]]/text()',
    )
    h.apply_date(person, "birthDate", squash_spaces(dob))

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
        name="Member of the Grand and General Council of San Marino",
        country="sm",
        wikidata_id="Q20968670",
        lang="eng",
    )

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The page declares UTF-8 but is actually served as ISO-8859-1 (Latin-1); decoding
    # it as anything else mojibakes the accented names.
    doc = context.fetch_html(
        context.data_url, encoding="latin-1", cache_days=1, absolute_links=True
    )
    for member in h.xpath_elements(doc, '//div[@class="membro"]'):
        crawl_member(context, member, position, categorisation)
