from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


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


def crawl_member(
    context: Context,
    url: str,
    mp_position: Entity,
    mp_categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=7)

    h1_els = h.xpath_elements(doc, ".//div[@class='section-head']/h1")
    raw_name = h.element_text(h1_els[0])

    if not raw_name:
        context.log.warning("Empty H1. No name. Skipping", url=url)
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
        context.log.warning("No electorate found, skipping", url=url, name=raw_name)
        return

    if fields:
        context.log.warning(
            "Unexpected profile fields", url=url, fields=list(fields.keys())
        )

    slug = url.rstrip("/").split("/")[-1]
    is_governor = slug.startswith("governor-")

    if is_governor:
        electorate_key = electorate.strip()
        if electorate_key not in GOVERNOR_POSITIONS:
            context.log.warning(
                "Unknown governor electorate", electorate=electorate_key, url=url
            )
            return
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
    else:
        position = mp_position
        cat = mp_categorisation

    if not cat.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_id("person", url, raw_name)

    h.apply_reviewed_name_string(
        context, person, string=raw_name, llm_cleaning=True, lang="eng"
    )

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
    mp_position = h.make_position(
        context,
        name="Member of the National Parliament of Papua New Guinea",
        country="pg",
        wikidata_id="Q21294916",
    )
    mp_categorisation = categorise(context, mp_position, default_is_pep=True)
    context.emit(mp_position)

    doc = context.fetch_html(context.data_url, cache_days=1)

    for a_el in h.xpath_elements(doc, ".//a[contains(@href,'parliament/bio/view/')]"):
        href_val = a_el.get("href", "")
        if "cat_url_title" in href_val:
            continue  # skip template placeholder
        if href_val.startswith("https"):
            url = href_val
        else:
            url = f"{context.data_url}index.php{href_val}"

        crawl_member(
            context,
            url,
            mp_position,
            mp_categorisation,
        )
