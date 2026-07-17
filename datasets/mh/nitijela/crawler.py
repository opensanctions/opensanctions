import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Each member is a heading "<n>. <role> - <honorific> <name>", e.g.
# "17. Senator - Honorable Daisy Alik Momotaro".
HEADING_RE = re.compile(r"^\d+\.\s*(?P<role>.+?)\s+-\s+(?P<name>.+)$")

# Honorifics prefixing the name. Stripped repeatedly so e.g. "Her Excellency Dr." or
# "Honorable Minister" are fully removed, leaving the person's actual name.
HONORIFIC_RE = re.compile(
    r"^(?:Her Excellency|His Excellency|Honorable|Hon\.|Minister|Senator|Dr\.)\s+"
)

# The role portion always begins with one of these. Anything else signals a layout or
# content change and should fail loudly.
ROLE_PREFIXES = ("President", "Speaker", "Vice Speaker", "Minister", "Senator")


def clean_name(raw: str) -> str:
    # A stray "|icon" template artifact trails some names.
    name = raw.split("|")[0].strip()
    while True:
        stripped = HONORIFIC_RE.sub("", name)
        if stripped == name:
            return stripped.strip()
        name = stripped


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    heading: str,
) -> None:
    match = HEADING_RE.match(heading)
    if match is None:
        raise ValueError(f"Unexpected member heading: {heading!r}")
    role = match.group("role").strip()
    if not role.startswith(ROLE_PREFIXES):
        raise ValueError(f"Unexpected role in heading: {heading!r}")
    name = clean_name(match.group("name"))
    assert name, f"Empty member name in heading: {heading!r}"

    person = context.make("Person")
    person.id = context.make_id(name, role)
    person.add("name", name)
    # A member of the Nitijela must be a citizen of the Republic: candidacy is limited to
    # qualified voters (Constitution Art. IV §4), and only citizens may vote (Art. IV §3).
    # https://www.constituteproject.org/constitution/Marshall_Islands_1995
    person.add("citizenship", "mh")

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
        name="Member of the Nitijela",
        country="mh",
        wikidata_id="Q21328624",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    headings = [
        " ".join(h.element_text(el).split()) for el in h.xpath_elements(doc, "//h3")
    ]
    members = [text for text in headings if HEADING_RE.match(text)]
    if not members:
        raise ValueError("No member headings found on the Nitijela members page")
    for heading in members:
        crawl_member(context, position, categorisation, heading)
