import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The two member listings that together make up the Legislative Assembly.
MEMBER_SECTIONS = ("peoples-representatives", "nobles-representatives")

# The parliament site returns HTTP 406 unless requests look like a browser.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# People's Representatives carry the honorific "Hon." (and sometimes "Dr"). Nobles are
# listed by their noble title ("Lord ...", "Prince ..."), which is their identifier and
# is kept; only the pure honorific "HSH" (His Serene Highness) is stripped.
HONORIFIC_RE = re.compile(r"^(?:Hon|Dr|HSH)\.?\s+")


def clean_name(raw: str) -> str:
    name = " ".join(raw.split())
    while True:
        stripped = HONORIFIC_RE.sub("", name)
        if stripped == name:
            return stripped.strip()
        name = stripped


def crawl_section(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    section: str,
) -> int:
    url = f"https://www.parliament.gov.to/en/members/{section}"
    doc = context.fetch_html(url, headers=HEADERS, cache_days=1)
    members: dict[str, str] = {}
    for link in h.xpath_elements(doc, f'//a[contains(@href, "/members/{section}/")]'):
        href = link.get("href")
        assert href is not None
        slug = href.rstrip("/").split("/")[-1]
        if not slug or slug == section:
            continue
        members[slug] = clean_name(h.element_text(link))

    for slug, name in members.items():
        assert name, f"Empty member name for {slug!r}"
        person = context.make("Person")
        person.id = context.make_slug(slug)
        person.add("name", name)
        person.add("sourceUrl", f"{url}/{slug}")
        # A member of the Legislative Assembly must be a Tongan subject: candidacy is
        # restricted to qualified electors (Constitution cl. 65), who must be Tongan
        # subjects (cl. 64). https://www.constituteproject.org/constitution/Tonga_2013
        person.add("citizenship", "to")

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
    return len(members)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Tonga",
        country="to",
        wikidata_id="Q21328621",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    total = 0
    for section in MEMBER_SECTIONS:
        total += crawl_section(context, position, categorisation, section)
    if total == 0:
        raise ValueError("No members found in the Tonga Legislative Assembly listings")
