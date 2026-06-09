from urllib.parse import urlparse

from lxml import etree
from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract import zyte_api
from zavod.util import Element

BASE_URL = "https://www.parliament.tas.gov.au"
HA_MEMBERS_URI = "/house-of-assembly/currentmembers"
LC_MEMBERS_URI = "/legislative-council/members"


def parse_party(doc: Element) -> tuple[str | None, str | None]:
    """
    Search for party pattern and return party and
    electoral position if exists
    """
    for ptag in h.xpath_elements(doc, '//main[@id="main"]//p'):
        text = etree.tostring(ptag, method="text", encoding="unicode")
        if "member for" in text:
            member_list = text.split("member for")
            if len(member_list) == 2:
                # party and jurisdiction
                return member_list[0].strip(), member_list[1].strip()
            elif len(member_list) == 1:
                # only jurisdiction
                return None, member_list[0].strip()
    return None, None


def parse_name(url: str) -> str:
    """
    Extract name from profile url. The URL slug is
    the most straight forward way to extract clean name
    """
    name_slug = url.split("/")[-1]
    name_list = name_slug.split("-")
    return " ".join(name_list).title()


def crawl_person(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    profile_url: str,
) -> None:
    doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator=".//h1",
        cache_days=1,
    )

    name = parse_name(profile_url)
    party, jurisdiction = parse_party(doc)

    slug = urlparse(profile_url).path.rstrip("/").rsplit("/", 1)[-1]
    person = context.make("Person")
    person.id = context.make_slug("member", slug)
    person.add("name", name)
    person.add("citizenship", "au")
    person.add("political", party)
    person.add("sourceUrl", profile_url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        propagate_country=True,
    )
    if occupancy is not None:
        if jurisdiction is not None:
            occupancy.add("constituency", jurisdiction)
        context.emit(occupancy)
        context.emit(person)


def crawl_chamber(
    context: Context,
    member_path_prefix: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    listing_url = BASE_URL + member_path_prefix
    doc = zyte_api.fetch_html(
        context,
        listing_url,
        unblock_validator=f".//a[contains(@href, '{member_path_prefix}')]",
        absolute_links=True,
        cache_days=1,
    )
    seen = set()
    for href in h.xpath_strings(doc, ".//a/@href"):
        parsed = urlparse(href)
        path = parsed.path.rstrip("/")

        # exclude non member links
        if not path.startswith(member_path_prefix):
            continue
        # exclude links without name
        if path.endswith(member_path_prefix):
            continue
        # exclude duplicates
        if path in seen:
            continue
        crawl_person(context, position, categorisation, BASE_URL + path)
        seen.add(path)


def crawl(context: Context) -> None:
    ha_position = h.make_position(
        context,
        name="Member of the House of Assembly of Tasmania",
        country="au",
        subnational_area="Tasmania",
        wikidata_id="Q19007285",
    )
    ha_categorisation = categorise(context, ha_position, default_is_pep=True)
    context.emit(ha_position)

    lc_position = h.make_position(
        context,
        name="Member of the Legislative Council of Tasmania",
        country="au",
        subnational_area="Tasmania",
        wikidata_id="Q19299542",
    )
    lc_categorisation = categorise(context, lc_position, default_is_pep=True)
    context.emit(lc_position)

    crawl_chamber(
        context,
        HA_MEMBERS_URI,
        ha_position,
        ha_categorisation,
    )
    crawl_chamber(
        context,
        LC_MEMBERS_URI,
        lc_position,
        lc_categorisation,
    )
