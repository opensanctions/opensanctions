import re

from lxml import etree
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)


# The Chamber's open-data SOAP service. The dataset's `data.url` enumerates the
# legislative periods; we then fetch each period's roster here
DEPUTIES_URL = (
    "https://opendata.camara.cl/camaradiputados/WServices"
    "/WSDiputado.asmx/retornarDiputadosXPeriodo"
)
# Public member page sits behind Cloudflare — hence Zyte.
# Only current members are fetched: the page shows their present district and email.
DETAIL_URL = "https://www.camara.cl/diputados/detalle/biografia.aspx?prmId=%s"
DISTRICT_RE = re.compile(r"Distrito:\s*N[ºo°]?\s*(\d+)")


def decode_cfemail(enc: str) -> str:
    """Decode a Cloudflare-obfuscated email (`data-cfemail`): each byte is XORed
    with the first byte, which is the key."""
    key = int(enc[:2], 16)
    return "".join(chr(int(enc[i : i + 2], 16) ^ key) for i in range(2, len(enc), 2))


def extract_email(doc: etree._Element) -> str | None:
    """Return the deputy's email, decoding Cloudflare obfuscation if present.

    When Zyte renders the page the protection script rewrites the address into a
    `mailto:` link; if it does not run, the obfuscated `data-cfemail` is decoded.
    """
    for href in h.xpath_strings(doc, './/a[starts-with(@href, "mailto:")]/@href'):
        addr = href.split("mailto:", 1)[1].split("?")[0].strip()
        if addr:
            return addr
    for enc in h.xpath_strings(doc, './/span[@class="__cf_email__"]/@data-cfemail'):
        if enc:
            return decode_cfemail(enc)
    return None


def crawl_detail(
    context: Context, person: Entity, occupancy: Entity, dip_id: str
) -> None:
    """Enrich a current member from their public profile page (via Zyte)."""
    url = DETAIL_URL % dip_id
    doc = zyte_api.fetch_html(
        context, url, unblock_validator='.//p[contains(., "Distrito:")]', cache_days=14
    )
    email = extract_email(doc)
    if email is None:
        context.log.warning("No email on deputy profile", url=url)
    else:
        person.add("email", email)

    # Match the whole paragraph's text, like the unblock validator does: the label and
    # the number are not guaranteed to sit in the same text node.
    paragraphs = h.xpath_elements(doc, './/p[contains(., "Distrito:")]')
    match = DISTRICT_RE.search(" ".join(h.element_text(p) for p in paragraphs))
    if match is None:
        context.log.warning("No district on deputy profile", url=url)
    else:
        occupancy.add("constituency", match.group(1))


def fetch_xml(context: Context, name: str, url: str, title: str) -> etree._Element:
    """Fetch a SOAP XML document, archive it, and return its namespace-stripped root."""
    path = context.fetch_resource(name, url)
    context.export_resource(path, XML, title=title)
    doc = context.parse_resource_xml(path)
    h.remove_namespace(doc)
    return doc.getroot()


def parties(deputy: etree._Element) -> list[str]:
    """Return the distinct party names the deputy was affiliated with, in document order."""
    names: list[str] = []
    for mil in deputy.findall(".//Militancia"):
        name = mil.findtext("Partido/Nombre")
        if name and name not in names:
            names.append(name)
    return names


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    deputy: etree._Element,
    period_start: str,
    period_end: str,
) -> None:
    dip_id = h.xpath_string(deputy, "Id/text()")
    first = " ".join(
        p for p in (deputy.findtext("Nombre"), deputy.findtext("Nombre2")) if p
    )
    last = " ".join(
        p
        for p in (
            deputy.findtext("ApellidoPaterno"),
            deputy.findtext("ApellidoMaterno"),
        )
        if p
    )
    assert first or last, f"Deputy {dip_id} without a name"

    person = context.make("Person")
    person.id = context.make_slug(dip_id)
    h.apply_name(person, first_name=first, last_name=last, lang="spa")
    person.add("gender", deputy.findtext("Sexo"))
    birth = deputy.findtext("FechaNacimiento")
    if birth is not None:
        h.apply_date(person, "birthDate", birth[:10])
    for party in parties(deputy):
        person.add("political", party, lang="spa")
    # Deputies must be citizens with the right to vote (Constitution of Chile,
    # Article 48). https://www.constituteproject.org/constitution/Chile_2021
    person.add("citizenship", "cl")
    person.add("sourceUrl", DETAIL_URL % dip_id)

    # Pass the term as period_start/period_end (not end_date): a past period end
    # yields an `ended` occupancy, while the ongoing term (period end in the future)
    # is treated as `current`. Occupancies older than the after-office window are
    # dropped by make_occupancy, so out-of-window periods emit no person.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # Only current members' profile pages are fetched: the page reflects the present
    # term's district and email, which would be wrong to attach to a past occupancy.
    if OccupancyStatus.CURRENT.value in occupancy.get("status"):
        crawl_detail(context, person, occupancy, dip_id)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Chile",
        country="cl",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18067639",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    periods = fetch_xml(
        context, "periods.xml", context.data_url, title="Legislative periods"
    )
    period_els = periods.findall(".//PeriodoLegislativo")
    for period in period_els:
        period_id = h.xpath_string(period, "Id/text()")
        name = h.xpath_string(period, "Nombre/text()")
        period_start = h.xpath_string(period, "FechaInicio/text()")[:10]
        period_end = h.xpath_string(period, "FechaTermino/text()")[:10]

        if period_start < h.earliest_term_start(position.get("topics")):
            continue

        deputies_root = fetch_xml(
            context,
            f"deputies_{period_id}.xml",
            f"{DEPUTIES_URL}?prmPeriodoID={period_id}",
            title=f"Deputies of the {name} period",
        )
        deputies = deputies_root.findall(".//Diputado")
        if not deputies:
            raise ValueError(f"No deputies found for period {name!r}")
        for deputy in deputies:
            crawl_member(
                context, position, categorisation, deputy, period_start, period_end
            )
