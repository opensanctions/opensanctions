import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Names carry honorific prefixes: "Hon." for members, "H.E" for the President, and
# occasionally "Dr". Strip them (repeatedly, e.g. "Hon. Dr ...") so the emitted name is
# the person's actual name. The matcher normalises case anyway.
HONORIFIC_RE = re.compile(r"^\s*(?:Hon|H\.E|Dr)\.?\s*", re.IGNORECASE)


def clean_name(raw: str) -> str:
    name = raw
    while True:
        stripped = HONORIFIC_RE.sub("", name)
        if stripped == name:
            return stripped.strip()
        name = stripped


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str | None],
) -> None:
    raw_name = row.pop("member")
    assert raw_name is not None, "Missing member name"
    name = clean_name(raw_name)
    assert name, "Empty member name"
    constituency = row.pop("constituency")
    assert constituency is not None, "Missing constituency"

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name)
    person.add("political", row.pop("party"))
    # A member of the Maneaba ni Maungatabu must be a citizen of Kiribati under
    # Chapter V, Section 55(a) of the Constitution of Kiribati.
    # https://www.constituteproject.org/constitution/Kiribati_2013
    person.add("citizenship", "ki")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.audit_data(row)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Maneaba ni Maungatabu",
        country="ki",
        wikidata_id="Q21296447",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    table = h.xpath_element(
        doc, '//table[.//tr[1][.//*[contains(text(), "Constituency")]]]'
    )
    rows = list(h.parse_html_table(table))
    if not rows:
        raise ValueError("No member rows found in the Kiribati members table")
    for row in rows:
        cells = h.cells_to_str(row)
        crawl_member(context, position, categorisation, cells)
