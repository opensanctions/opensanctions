import re
from dataclasses import dataclass

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Senator profile links on the listing pages, e.g.
#   /www/?MIval=showSenator&ID=4689&LANG=nl
SENATOR_ID_RE = re.compile(r"MIval=showSenator&ID=(\d+)", re.IGNORECASE)

# Per-legislature listing links, e.g.
#   /www/?MIval=WieIsWie/SenPerType&LEG=7&LANG=nl  -> "Legislatuur 2019-2024"
LEG_ID_RE = re.compile(r"SenPerType&LEG=(\d+)", re.IGNORECASE)
LEG_LABEL_RE = re.compile(r"Legislatuur\s+(\d{4})\s*-\s*(\d{4}|\.+)")

# Born line in the Dutch biography, e.g.
#   "Geboren te Roeselare op 20 september 1982"
BORN_RE = re.compile(
    r"Geboren\s+te\s+(?P<place>.+?)\s+op\s+(?P<date>\d{1,2}\s+\w+\s+\d{4})",
    re.IGNORECASE | re.DOTALL,
)

DETAIL_URL = "https://www.senate.be/www/?MIval=showSenator&ID=%s&LANG=nl"
LEG_URL = "https://www.senate.be/www/?MIval=WieIsWie/SenPerType&LEG=%s&LANG=nl"


@dataclass
class Legislature:
    leg: str
    start: str
    end: str | None


def crawl_senator(
    context: Context,
    senator_id: str,
    legislature: Legislature,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    url = DETAIL_URL % senator_id
    doc = context.fetch_html(url, cache_days=7)

    name = h.xpath_string(doc, "//title/text()").strip()
    if not name:
        context.log.warning("Senator without name", senator_id=senator_id)
        return

    # First heading reads "<name> - <political group>".
    heading = h.element_text(h.xpath_element(doc, "(//th)[1]"))
    political = None
    if heading is not None and " - " in heading:
        political = heading.split(" - ", 1)[1].strip()

    body_text = h.element_text(h.xpath_element(doc, "//body"))
    birth_place = None
    birth_date = None
    if body_text is not None:
        match = BORN_RE.search(body_text)
        if match is not None:
            birth_place = match.group("place").strip()
            birth_date = match.group("date").strip()

    person = context.make("Person")
    person.id = context.make_slug(senator_id)
    person.add("name", name)
    person.add("political", political)
    person.add("birthPlace", birth_place)
    # Art. 69 of the Belgian Constitution requires senators to "be Belgian":
    # https://www.dekamer.be/kvvcr/pdf_sections/publications/constitution/grondwetuk.pdf
    person.add("citizenship", "be")
    person.add("sourceUrl", url)
    h.apply_date(person, "birthDate", birth_date)

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        categorisation=categorisation,
        no_end_implies_current=legislature.end is None,
        period_start=legislature.start,
        period_end=legislature.end,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl_legislature(
    context: Context,
    legislature: Legislature,
    cutoff: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    if legislature.start < cutoff:
        context.log.info(
            "Skipping legislature outside PEP relevance window",
            leg=legislature.leg,
            start=legislature.start,
        )
        return

    doc = context.fetch_html(LEG_URL % legislature.leg, cache_days=1)
    senator_ids = sorted(
        {
            match.group(1)
            for match in (
                SENATOR_ID_RE.search(href) for href in h.xpath_strings(doc, "//a/@href")
            )
            if match is not None
        }
    )
    if not senator_ids:
        raise ValueError(f"No senator links for legislature {legislature.leg}")

    for senator_id in senator_ids:
        crawl_senator(context, senator_id, legislature, position, categorisation)


def parse_legislatures(context: Context) -> list[Legislature]:
    doc = context.fetch_html(context.data_url, cache_days=1)
    legislatures: dict[str, Legislature] = {}
    for anchor in h.xpath_elements(doc, "//a[contains(@href, 'SenPerType&LEG=')]"):
        href = anchor.get("href")
        leg_match = LEG_ID_RE.search(href or "")
        label_match = LEG_LABEL_RE.search(h.element_text(anchor) or "")
        if leg_match is None or label_match is None:
            continue
        leg = leg_match.group(1)
        end_raw = label_match.group(2)
        end = end_raw if end_raw.isdigit() else None
        legislatures[leg] = Legislature(leg=leg, start=label_match.group(1), end=end)
    return list(legislatures.values())


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Belgium",
        wikidata_id="Q17619252",
        country="be",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    cutoff = h.earliest_term_start(position.get("topics"))

    legislatures = parse_legislatures(context)
    if not legislatures:
        raise ValueError("No legislature listings found on the index page")

    for legislature in legislatures:
        crawl_legislature(context, legislature, cutoff, position, categorisation)
