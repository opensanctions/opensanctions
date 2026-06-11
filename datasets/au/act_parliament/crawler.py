import re
from urllib.parse import urlparse

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract import zyte_api
from zavod.util import Element

MEMBERS_URL = "https://www.parliament.act.gov.au/members/current"
# The first "<day> <Month> <year>" in the electoral-history prose is the date the
# member was first elected; the "returned to the ... Assemblies" sentences carry no
# dates.
DATE_RE = re.compile(r"\b(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})\b")


def first_elected_date(context: Context, detail_url: str) -> str | None:
    """Fetch a member's detail page and return the date they were first elected.

    The Assembly website has no structured term field, so we mine it from the
    free-text "Electoral history" paragraph. Returns None (and warns) when the
    paragraph is missing or carries no recognisable date, so the occupancy can
    still be emitted without a start date.
    """
    doc = zyte_api.fetch_html(
        context, detail_url, unblock_validator=".//h1", cache_days=7
    )
    heading = h.xpath_element(doc, ".//h2[normalize-space()='Electoral history']")
    para = heading.getnext()
    text = h.element_text(para) if para is not None else ""
    match = DATE_RE.search(text)
    if match is None:
        context.log.warning("No elected date in electoral history", url=detail_url)
        return None
    return match.group(1)


def crawl_member(
    context: Context,
    row: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    cells = row.findall("./td")
    if len(cells) != 4:
        context.log.warning("Unexpected column count", count=len(cells))
        return
    member_cell, electorate_cell, affiliation_cell, _contact = cells

    link = h.xpath_element(member_cell, "./a[contains(@href, '/members/current/')]")
    detail_url = link.get("href")
    assert isinstance(detail_url, str), detail_url
    name = h.element_text(link)  # "Andrew Barr" — the preceding <img> has an empty alt
    slug = urlparse(detail_url).path.rstrip("/").rsplit("/", 1)[-1]

    electorate = h.element_text(electorate_cell)  # "Kurrajong"
    party = h.element_text(affiliation_cell).lstrip("⬤").strip()  # "Labor"

    person = context.make("Person")
    person.id = context.make_slug("member", slug)
    person.add("name", name)
    # Australian citizenship is a legal precondition for ACT MLAs:
    # Electoral Act 1992 (ACT) s 103(1)(a). https://www.legislation.act.gov.au/a/1992-71/
    person.add("citizenship", "au")
    person.add("political", party)
    person.add("sourceUrl", detail_url)

    # Service is described as continuous across assemblies ("returned to the ...
    # Assemblies"), so the first-elected date is the start of one ongoing occupancy.
    start_date = first_elected_date(context, detail_url)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", electorate)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Australian Capital Territory Legislative Assembly",
        country="au",
        subnational_area="Australian Capital Territory",
        wikidata_id="Q6814365",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        MEMBERS_URL,
        unblock_validator=".//table//a[contains(@href, '/members/current/')]",
        absolute_links=True,
        cache_days=1,
    )
    table = h.xpath_element(doc, ".//table[.//th[normalize-space()='Affiliation']]")
    rows = h.xpath_elements(table, ".//tbody/tr")
    if not (20 <= len(rows) <= 30):
        # The Assembly has 25 seats; a wild count means the page layout changed.
        context.log.warning("Unexpected member row count", count=len(rows))
    for row in rows:
        crawl_member(context, row, position, categorisation)
