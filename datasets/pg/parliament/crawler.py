import re
from typing import Any

from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

BASE_URL = "https://www.parliament.gov.pg"

# The term segment embedded in every member bio URL.
# A new parliament changes this to "twelfth-parliament" etc.; raise so the
# crawler is updated rather than silently producing stale data.
PARLIAMENT_TERM = "eleventh-parliament"

# Q21294916 — "Member of the National Parliament of Papua New Guinea"
MP_POSITION_QID = "Q21294916"

# Provincial governor positions: electorate text from profile → (province label, QID)
# QIDs sourced from Wikidata search June 2026; absent where no item exists.
# Constitutional basis for citizenship: Constitution of PNG s.50(1)(ba), s.56(1), s.103(3)(a).
# https://www.parliament.gov.pg/images/misc/PNG-CONSTITUTION.pdf
GOVERNOR_POSITIONS: dict[str, tuple[str, str | None]] = {
    "Bougainville Provincial": ("Bougainville Province", None),
    "Central Provincial": ("Central Province", "Q130752107"),
    "Chimbu Provincial": ("Chimbu Province", "Q131438844"),
    "East New Britain Provincial": ("East New Britain Province", "Q131432439"),
    "East Sepik Provincial": ("East Sepik Province", "Q131445402"),
    "Eastern Highlands Provincial": ("Eastern Highlands Province", "Q131438812"),
    "Enga Provincial": ("Enga Province", None),
    "Gulf Provincial": ("Gulf Province", "Q131454260"),
    "Hela Provincial": ("Hela Province", "Q131438883"),
    "Jiwaka Provincial": ("Jiwaka Province", "Q131440658"),
    "Madang Provincial": ("Madang Province", "Q131454352"),
    "Manus Provincial": ("Manus Province", "Q131443494"),
    "Milne Bay Provincial": ("Milne Bay Province", "Q131454932"),
    "Morobe Provincial": ("Morobe Province", "Q131457587"),
    "National Capital District": ("National Capital District", None),
    "New Ireland Provincial": ("New Ireland Province", None),
    "Northern Provincial": ("Northern Province", "Q131454130"),
    "Southern Highlands Provincial": ("Southern Highlands Province", "Q131273660"),
    "West New Britain Provincial": ("West New Britain Province", "Q131459286"),
    "West Sepik Provincial": ("West Sepik Province", None),
    "Western Provincial": ("Western Province", "Q131418994"),
    "Western Highlands Provincial": ("Western Highlands Province", "Q131441010"),
}


def normalize_bio_url(href: str) -> str:
    """Return a canonical bio URL: always uses the /index.php/ prefix."""
    if "parliament.gov.pg" in href:
        path = "/" + href.split("parliament.gov.pg", 1)[1].lstrip("/")
    else:
        path = href
    if not path.startswith("/index.php/"):
        path = "/index.php" + path
    return BASE_URL + path


def extract_profile_fields(div: Element) -> dict[str, str]:
    """Parse <p><strong>Key</strong><br>Value</p> pairs from the profile div."""
    fields: dict[str, str] = {}
    for p_el in h.xpath_elements(div, ".//p"):
        strong_els = h.xpath_elements(p_el, "strong")
        if not strong_els or not strong_els[0].text:
            continue
        key = strong_els[0].text.strip()
        br_els = h.xpath_elements(p_el, "br")
        if br_els and br_els[0].tail:
            value = br_els[0].tail.strip()
            if value:
                fields[key] = value
    return fields


def clean_name(raw: str) -> str:
    name = re.sub(r",.*$", "", raw)
    for title in ["Hon.", "Hon.Sir", "Rt.", "Sir.", "Sir", "Dr.", "Hon.Mai"]:
        name = name.replace(title, "", 1)
    return name.strip()


def crawl_member(
    context: Context,
    url: str,
    slug: str,
    is_governor: bool,
    mp_position: Entity,
    mp_categorisation: PositionCategorisation,
    governor_cache: dict[str, tuple[Entity, PositionCategorisation]],
) -> None:
    doc = context.fetch_html(url, cache_days=7)

    h1_els = h.xpath_elements(doc, ".//div[@class='section-head']/h1")
    if not h1_els:
        context.log.warning("No H1 found, skipping", url=url)
        return
    raw_name = h.element_text(h1_els[0])
    name = clean_name(raw_name)
    if not name:
        context.log.warning("Empty H1, skipping", url=url)
        return

    # Profile fields are in the col-md-7 div inside the first section-body
    profile_divs = h.xpath_elements(
        doc,
        ".//div[@class='section-body'][1]//div[contains(@class,'col-md-7')]",
    )
    if not profile_divs:
        context.log.warning("No profile column found, skipping", url=url)
        return
    fields = extract_profile_fields(profile_divs[0])

    electorate = fields.pop("Electorate", None)
    party = fields.pop("Party", None)
    election_date_raw = fields.pop("Date of Election", None)
    # Not captured: votes, ministerial/shadow roles (executive government, not PEP position)
    fields.pop("Votes Received", None)
    fields.pop("Ministerial Portfolio", None)
    fields.pop("Shadow Minister for", None)

    if electorate is None:
        context.log.warning("No electorate found, skipping", url=url, name=name)
        return

    if fields:
        context.log.warning(
            "Unexpected profile fields", url=url, fields=list(fields.keys())
        )

    if is_governor:
        electorate_key = electorate.strip()
        if electorate_key not in GOVERNOR_POSITIONS:
            context.log.warning(
                "Unknown governor electorate", electorate=electorate_key, url=url
            )
            return
        if electorate_key not in governor_cache:
            province_name, wikidata_id = GOVERNOR_POSITIONS[electorate_key]
            position = h.make_position(
                context,
                name=f"Governor of {province_name}",
                country="pg",
                subnational_area=province_name,
                wikidata_id=wikidata_id,
            )
            cat = categorise(context, position, default_is_pep=True)
            context.emit(position)
            governor_cache[electorate_key] = (position, cat)
        position, cat = governor_cache[electorate_key]
    else:
        position = mp_position
        cat = mp_categorisation

    if not cat.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_slug(slug)
    person.add("name", name)
    person.add("political", party)
    # PNG Constitution s.50(1)(ba) and s.56(1) bar non-citizens and dual citizens
    # from elective public office, including both Open and Provincial seats.
    # https://www.parliament.gov.pg/images/misc/PNG-CONSTITUTION.pdf
    person.add("citizenship", "pg")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=cat,
        election_date=election_date_raw,
    )
    if occupancy is not None:
        occupancy.add("constituency", electorate)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    # Collect unique bio links; enforce that all belong to the expected parliament term.
    bio_links: list[tuple[str, str, bool]] = []  # (url, slug, is_governor)
    seen_urls: set[str] = set()

    # Detect links to bio pages that don't belong to the expected parliament term.
    # Fires when "twelfth-parliament" links appear, signalling the crawler needs updating.
    for a_el in h.xpath_elements(doc, ".//a[contains(@href,'parliament/bio/view/')]"):
        href_val: Any = a_el.get("href", "")
        if not isinstance(href_val, str):
            continue
        # Skip the Joomla footer template placeholder ({cat_url_title}).
        if "{" in href_val:
            continue
        if PARLIAMENT_TERM not in href_val:
            raise ValueError(
                f"Unexpected parliament term in link: {href_val!r} "
                f"(expected {PARLIAMENT_TERM!r} — a new parliament may have been formed)"
            )

    for a_el in h.xpath_elements(
        doc, f".//a[contains(@href, '{PARLIAMENT_TERM}/bio/view/')]"
    ):
        href: Any = a_el.get("href", "")
        if not isinstance(href, str) or not href:
            continue
        url = normalize_bio_url(href)
        if url in seen_urls:
            continue
        seen_urls.add(url)
        slug = url.rstrip("/").split("/")[-1]
        is_governor = slug.startswith("governor-")
        bio_links.append((url, slug, is_governor))

    if not bio_links:
        raise ValueError(
            "No MP bio links found on homepage — site structure may have changed"
        )

    context.log.info(f"Found {len(bio_links)} member bio links")

    mp_position = h.make_position(
        context,
        name="Member of the National Parliament of Papua New Guinea",
        country="pg",
        wikidata_id=MP_POSITION_QID,
    )
    mp_categorisation = categorise(context, mp_position, default_is_pep=True)
    context.emit(mp_position)

    governor_cache: dict[str, tuple[Entity, PositionCategorisation]] = {}

    for url, slug, is_governor in bio_links:
        crawl_member(
            context,
            url,
            slug,
            is_governor,
            mp_position,
            mp_categorisation,
            governor_cache,
        )
