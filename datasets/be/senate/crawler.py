import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

POSITION_TOPICS = ["gov.legislative", "gov.national"]

# Senator profile links on the listing page, e.g.
#   /www/?MIval=showSenator&ID=4689&LANG=nl
SENATOR_ID_RE = re.compile(r"MIval=showSenator&ID=(\d+)", re.IGNORECASE)

# Born line in the Dutch biography, e.g.
#   "Geboren te Roeselare op 20 september 1982"
BORN_RE = re.compile(
    r"Geboren\s+te\s+(?P<place>.+?)\s+op\s+(?P<date>\d{1,2}\s+\w+\s+\d{4})",
    re.IGNORECASE | re.DOTALL,
)

DETAIL_URL = "https://www.senate.be/www/?MIval=showSenator&ID=%s&LANG=nl"


def crawl_senator(context: Context, senator_id: str) -> None:
    url = DETAIL_URL % senator_id
    doc = context.fetch_html(url, cache_days=7)

    name = h.xpath_string(doc, "//title/text()").strip()
    if not name:
        context.log.warning("Senator without name", senator_id=senator_id)
        return

    # First heading reads "<name> - <political group>".
    heading = h.element_text(h.xpath_element(doc, "(//th)[1]"))
    political = None
    if " - " in heading:
        political = heading.split(" - ", 1)[1].strip()

    body_text = h.element_text(h.xpath_element(doc, "//body"))
    birth_place = None
    birth_date = None
    match = BORN_RE.search(body_text)
    if match is not None:
        birth_place = match.group("place").strip()
        birth_date = match.group("date").strip()

    person = context.make("Person")
    person.id = context.make_id(name, birth_date)
    person.add("name", name)
    person.add("political", political)
    person.add("birthPlace", birth_place)
    # Art. 69 of the Belgian Constitution requires senators to "be Belgian":
    # https://www.dekamer.be/kvvcr/pdf_sections/publications/constitution/grondwetuk.pdf
    person.add("citizenship", "be")
    person.add("sourceUrl", url)
    h.apply_date(person, "birthDate", birth_date)

    position = h.make_position(
        context,
        name="Member of the Senate of Belgium",
        wikidata_id="Q17619252",
        country="be",
        topics=POSITION_TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        categorisation=categorisation,
        no_end_implies_current=True,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    hrefs = h.xpath_strings(doc, "//a/@href")

    senator_ids = []
    seen = set()
    for href in hrefs:
        match = SENATOR_ID_RE.search(href)
        if match is None:
            continue
        senator_id = match.group(1)
        if senator_id in seen:
            continue
        seen.add(senator_id)
        senator_ids.append(senator_id)

    if not senator_ids:
        raise ValueError("No senator profile links found on the listing page")

    for senator_id in senator_ids:
        crawl_senator(context, senator_id)
