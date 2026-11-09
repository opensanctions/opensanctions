import re
from html import unescape

from lxml.etree import _Element as Element
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def decode_email(record: Element) -> str:
    # Joomla's email cloak spells the address out in JavaScript as entity-encoded
    # string literals: `var addy_text<hash> = '...' + '...';`
    script = h.xpath_string(record, ".//script/text()")
    match = re.search(r"var addy_text[0-9a-f]+ = ((?:'[^']*'(?: \+ )?)+);", script)
    assert match is not None, script
    return unescape("".join(re.findall(r"'([^']*)'", match.group(1))))


def crawl_record(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: Element,
) -> None:
    # Published surname-first ("Agudelo García Ana Paola"); compound surnames make
    # splitting it into given and family names ambiguous.
    name = h.element_text(h.xpath_element(record, ".//h3"))
    # The party paragraph follows the name. The office and the switchboard phone
    # extension below it are not extracted: phone numbers are excluded for PEPs.
    # The paragraphs carry no class, so a card without a party would put its office
    # in this place.
    party = h.element_text(h.xpath_element(record, ".//h3/following-sibling::p[1]"))

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    if party == "" or re.search(r"Edificio|Capitolio|Piso|Of\.", party):
        context.log.warning("Party missing or an office", name=name, party=party)
    else:
        person.add("political", party)
    for href in h.xpath_strings(record, ".//blockquote//a/@href"):
        person.add("sourceUrl", href)
    person.add("email", decode_email(record))
    # Senators must be Colombian by birth: Constitución Política de Colombia (1991),
    # Art. 172. http://www.secretariasenado.gov.co/senado/basedoc/constitucion_politica_1991_pr005.html
    person.add("citizenship", "co")

    # The roster lists only the sitting Senate and publishes no dates for it.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Colombia",
        wikidata_id="Q19254253",
        country="co",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    # The page carries the directory twice: a per-letter view, and an all-letters
    # ("Todos") view that holds every senator.
    results = h.xpath_element(
        doc, '//div[@class="modazdirectory__results"][h1="Todos"]'
    )
    for record in h.xpath_elements(
        results, './/div[contains(@class, "modazdirectory__result ")]'
    ):
        crawl_record(context, position, categorisation, record)
