import re
from typing import Any
from urllib.parse import quote

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# The Senate publishes its directory via a JSON API behind Cloudflare, so all
# requests are routed through Zyte. `/hq/congress/` lists every Congress; the
# one with the highest id is the current term, whose members we then fetch.
CONGRESS_LIST_URL = "https://senate.gov.ph/hq/congress/"
SENATORS_URL = "https://senate.gov.ph/hq/senators/congress/%s?per_page=100"

# Each Congress runs for three years, beginning on 30 June following the May
# elections. The 20th Congress (API id 28) convened on 30 June 2025; Congress
# ids are sequential, so any term's start year can be derived from this anchor.
ANCHOR_CONGRESS_ID = 28
ANCHOR_START_YEAR = 2025
CONGRESS_YEARS = 3

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def congress_term(congress_id: int) -> tuple[str, str]:
    """Return the (start_date, end_date) of a Congress as ISO date strings."""
    start_year = ANCHOR_START_YEAR + (congress_id - ANCHOR_CONGRESS_ID) * CONGRESS_YEARS
    return f"{start_year}", f"{start_year + CONGRESS_YEARS}"


def source_url(name: str) -> str:
    """Build the per-senator profile URL, e.g. .../senator/Win-Gatchalian."""
    return "https://senate.gov.ph/senator/" + quote(name.replace(" ", "-"))


def clean_biography(raw: str) -> str:
    """Strip the HTML biography down to plain text, keeping paragraph breaks."""
    doc = html.fromstring(raw)
    paragraphs = [h.element_text(el) for el in h.xpath_elements(doc, ".//p")]
    paragraphs = [p for p in paragraphs if p]
    if paragraphs:
        return "\n\n".join(paragraphs)
    return h.element_text(doc)


def crawl_senator(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    cutoff: str,
    senator: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(senator.pop("id")))
    name = senator.pop("name")
    h.apply_name(person, full=name)
    person.add("sourceUrl", source_url(name))
    # Free-text role label, e.g. "Senator" or a leadership title like
    # "Senate President Pro Tempore"; the Position entity always says "Senator".
    person.add("position", senator.pop("position"))

    # The Senate requires senators to be natural-born citizens of the
    # Philippines (1987 Constitution, Art. VI, Sec. 3 -- lawphil.net/consti/cons1987.html).
    person.add("citizenship", "ph")

    for entry in senator.pop("emails"):
        emails = EMAIL_RE.findall(entry)
        if not emails:
            context.log.warning("No email address found", value=entry, person=person.id)
        person.add("email", emails)

    for link in senator.pop("website_links") + senator.pop("social_media"):
        if "://" not in link:
            link = "https://" + link
        person.add("website", link)

    address = senator.pop("address").strip()
    if address:
        addr = h.make_address(context, full=address, country_code="ph")
        h.copy_address(person, addr)

    biography = senator.pop("biography")
    if biography is not None:
        person.add("notes", clean_biography(biography))

    # One occupancy per Congress the senator has served in, bounded to terms
    # recent enough to still matter for PEP screening.
    occupancies = []
    for congress_id in sorted(set(senator.pop("congress_ids")), key=int):
        start_date, end_date = congress_term(int(congress_id))
        if start_date < cutoff:
            continue
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=start_date,
            end_date=end_date,
        )
        if occupancy is not None:
            occupancies.append(occupancy)

    if not occupancies:
        return
    for occupancy in occupancies:
        context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        senator,
        ignore=[
            "description",
            "image_upload_id",
            "image_upload",
            "flag",
            "status",
            "lis_code",
            "biography",  # consumed above only when present
            "resume",
            "contact_numbers",  # office switchboard descriptions, not dialable numbers
            "stats",
            "congresses",
            "created_at",
            "updated_at",
        ],
    )


def crawl(context: Context) -> None:
    congresses = zyte_api.fetch_json(context, CONGRESS_LIST_URL, cache_days=1)
    latest = max(congresses, key=lambda c: int(c["id"]))
    context.log.info("Crawling current Congress", id=latest["id"], name=latest["name"])

    data = zyte_api.fetch_json(context, SENATORS_URL % latest["id"], cache_days=1)
    senators = data["senators"]
    if len(senators) < 18:
        context.log.warning("Fewer senators than expected", count=len(senators))

    position = h.make_position(
        context,
        name="Senator of the Philippines",
        country="ph",
        wikidata_id="Q18579098",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    cutoff = h.earliest_term_start(["gov.national"])
    for senator in senators:
        crawl_senator(context, position, categorisation, cutoff, senator)
