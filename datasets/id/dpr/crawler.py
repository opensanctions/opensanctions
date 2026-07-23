import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# The site is Akamai-protected and client-rendered, so each electoral-district (dapil) page
# is fetched through the Zyte API (browser rendering) with an Indonesian exit.
GEOLOCATION = "id"

# Members are elected from 84 electoral districts.
DAPIL_COUNT = 84
DAPIL_URL = "https://www.dpr.go.id/anggota/index/dapil/%d"

# Each member links to a detail page "/anggota/detail/id/<id>" (or "/anggota/id/<id>").
MEMBER_HREF_RE = re.compile(r"/anggota/(?:detail/)?id/(\d+)")
UNBLOCK_VALIDATOR = './/a[contains(@href, "/anggota/") and contains(@href, "id/")]'


def crawl_dapil(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    dapil: int,
    seen: set[str],
) -> None:
    url = DAPIL_URL % dapil
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=UNBLOCK_VALIDATOR,
        geolocation=GEOLOCATION,
        cache_days=1,
    )
    for link in h.xpath_elements(doc, "//a[@href]"):
        href = link.get("href")
        if href is None:
            continue
        match = MEMBER_HREF_RE.search(href)
        if match is None:
            continue
        member_id = match.group(1)
        name = h.element_text(link)
        if not name or member_id in seen:
            continue
        seen.add(member_id)

        person = context.make("Person")
        person.id = context.make_slug(member_id)
        person.add("name", name)
        person.add("sourceUrl", f"https://www.dpr.go.id/anggota/detail/id/{member_id}")
        # DPR candidates must be Indonesian citizens (Law No. 7 of 2017 on General
        # Elections, Article 240 paragraph (1)). https://peraturan.bpk.go.id/Details/37644
        person.add("citizenship", "id")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", f"Dapil {dapil}")
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's Representative Council of Indonesia",
        country="id",
        wikidata_id="Q21328632",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    seen: set[str] = set()
    for dapil in range(1, DAPIL_COUNT + 1):
        crawl_dapil(context, position, categorisation, dapil, seen)
    if not seen:
        raise ValueError("No DPR members found across the electoral districts")
