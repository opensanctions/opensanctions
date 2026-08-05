import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The source publishes only the sitting legislature and carries no per-member or per-term
# dates — just the legislature number in the page heading (e.g. "8ª Legislatura"). We map
# each legislature number to the date it took office, so occupancies can be dated. When a
# new legislature is seated the heading number changes; the crawler then fails loudly (see
# crawl()) until the new start date is recorded here, rather than silently dating members
# to the wrong term. The 8th Legislature took office on 16 October 2025 for a five-year
# term.
LEGISLATURE_START = {
    "8": "2025-10-16",
}

LEGISLATURE_RE = re.compile(r"(\d+)ª\s*Legislatura")

# Detail-page table labels. Only the name and birth-year rows are extracted; the office
# contact rows (email, website, etc.) are intentionally not captured (see audit_data).
LABEL_NAME_ZHO = "中文名字:"  # Chinese name
LABEL_NAME_LAT = "葡文名字:"  # Portuguese / romanised name
LABEL_BIRTH = "date of birth:"
# Office section header and known contact-detail labels, ignored on audit.
IGNORE_LABELS = ["議員辦事處資料", "LABEL_EMAIL:", "LABEL_WEBSITE:"]


def crawl_member(
    context: Context,
    url: str,
    deputy_id: str,
    listing_name: str,
    term_start: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=7)
    table = h.xpath_element(doc, ".//div[@class='deputies-detail']//table")

    # The detail page is a label/value table: each data row is `<td>label</td>
    # <td>value</td>`. Section headers and spacer rows have fewer than two cells and are
    # skipped here; the office section header is also listed in IGNORE_LABELS in case the
    # markup ever changes.
    row: dict[str, str] = {}
    for tr in h.xpath_elements(table, ".//tr"):
        cells = h.xpath_elements(tr, "./td")
        if len(cells) < 2:
            continue
        label = h.element_text(cells[0])
        if not label:
            continue
        row[label] = h.element_text(cells[1])

    name_zho = row.pop(LABEL_NAME_ZHO, "")
    name_lat = row.pop(LABEL_NAME_LAT, "")
    birth = row.pop(LABEL_BIRTH, "")

    person = context.make("Person")
    person.id = context.make_id(deputy_id, name_zho)
    person.add("name", name_zho, lang="zho")
    person.add("name", name_lat, lang="eng")
    person.add("name", listing_name, lang="eng")
    h.apply_date(person, "birthDate", birth)
    # Members of the Legislative Assembly are only legally required to be permanent
    # residents of Macau (Basic Law Art. 68); they need not be Chinese nationals — Chinese
    # nationality is required only of the President and Vice-President (Art. 72), and
    # members may hold other nationalities (e.g. Portuguese).
    # https://en.wikisource.org/wiki/Basic_Law_of_the_Macao_Special_Administrative_Region/Chapter_IV/Section_3
    person.add("country", "mo")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=term_start,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_LABELS)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)

    # Derive the sitting legislature from the page heading and look up its start date.
    heading = h.element_text(h.xpath_element(doc, ".//h3[contains(., 'Legislatura')]"))
    match = LEGISLATURE_RE.search(heading)
    if match is None:
        raise ValueError(f"Could not parse legislature from heading: {heading!r}")
    legislature = match.group(1)
    term_start = LEGISLATURE_START.get(legislature)
    if term_start is None:
        raise ValueError(
            f"Unknown legislature {legislature!r} — a new term has been seated; add its start date "
            "to LEGISLATURE_START."
        )

    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Macau",
        country="mo",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q28941940",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    links = h.xpath_elements(
        doc,
        ".//div[contains(@class, 'channel-deputy')]//a[contains(@href, '/deputies/')]",
    )

    seen: set[str] = set()
    for link in links:
        href = link.get("href")
        if href is None:
            raise ValueError("Deputy link without href")
        deputy_id = href.rstrip("/").rsplit("/", 1)[-1]
        if deputy_id in seen:
            continue
        seen.add(deputy_id)
        crawl_member(
            context,
            href,
            deputy_id,
            h.element_text(link),
            term_start,
            position,
            categorisation,
        )
