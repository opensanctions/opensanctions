import re

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The page declares UTF-8 but is actually served as ISO-8859-1 (Latin-1); decoding it as
# anything else mojibakes the accented names.
ENCODING = "latin-1"

SCHEDA_ID_RE = re.compile(r"scheda(\d+)\.html")


def crawl_member(
    context: Context,
    membro: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> bool:
    headings = h.xpath_elements(membro, ".//h3")
    if not headings:
        return False
    heading = headings[0]
    name = h.element_text(heading)
    # The roster carries an empty placeholder block with no councillor; skip it.
    if not name:
        return False

    # The councillor's own detail-page link lives in the heading; its scheda id is a
    # stable per-person identifier.
    scheda_id = None
    for href in h.xpath_strings(heading, ".//a/@href"):
        match = SCHEDA_ID_RE.search(href)
        if match is not None:
            scheda_id = match.group(1)
            break

    # The party is the direct-child link of the member block.
    party_links = h.xpath_elements(membro, "./a")
    party = h.element_text(party_links[0]) if party_links else None

    person = context.make("Person")
    person.id = (
        context.make_slug(scheda_id)
        if scheda_id is not None
        else context.make_id(name, party)
    )
    person.add("name", name)
    person.add("political", party, lang="ita")
    # Standing for the Council requires Sammarinese citizenship: candidates must meet
    # the elector conditions of the Electoral Law (Art. 4, native/naturalised
    # Sammarinese) plus Art. 21. https://www.consigliograndeegenerale.sm/on-line/home/composizione/elenco-consiglieri.html
    person.add("citizenship", "sm")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return False
    context.emit(occupancy)
    context.emit(person)
    return True


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Grand and General Council of San Marino",
        country="sm",
        wikidata_id="Q20968670",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, encoding=ENCODING, cache_days=1)
    count = 0
    for membro in h.xpath_elements(doc, '//div[@class="membro"]'):
        if crawl_member(context, membro, position, categorisation):
            count += 1

    if count == 0:
        raise ValueError("No councillors found in the Council roster")
