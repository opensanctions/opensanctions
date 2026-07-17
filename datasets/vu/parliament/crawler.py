import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# The parliament site is unreachable from ordinary clients (connections time out), so the
# page is fetched through the Zyte API. An Australian exit is used as it is the closest
# well-supported location to Vanuatu.
GEOLOCATION = "au"

# At least one row of the roster table must be present for the fetch to count as
# successfully unblocked.
TABLE_XPATH = './/table[contains(@class, "table-striped")]'

# Names carry the honorific "Hon." and use non-breaking spaces. The surname is written in
# upper case; we keep the source casing since the matcher normalises it.
HONORIFIC_RE = re.compile(r"^Hon\.\s*", re.IGNORECASE)


def clean_name(raw: str) -> str:
    return HONORIFIC_RE.sub("", " ".join(raw.split())).strip()


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str | None],
) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, "Missing member name"
    name = clean_name(raw_name)
    assert name, "Empty member name"
    constituency = row.pop("constituency")

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name)
    person.add("political", row.pop("party"))
    # Every citizen of Vanuatu at least 25 years of age is eligible to stand for
    # Parliament (Constitution of Vanuatu, Chapter 4, Article 17(2)).
    # https://www.constituteproject.org/constitution/Vanuatu_2013
    person.add("citizenship", "vu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.audit_data(row, ignore=["position_portfolio", "profile"])
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Vanuatu",
        country="vu",
        wikidata_id="Q21294920",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=TABLE_XPATH,
        geolocation=GEOLOCATION,
        cache_days=1,
    )
    table = h.xpath_element(doc, TABLE_XPATH)
    rows = list(h.parse_html_table(table))
    if not rows:
        raise ValueError("No member rows found in the Vanuatu members table")
    for row in rows:
        crawl_member(context, position, categorisation, h.cells_to_str(row))
