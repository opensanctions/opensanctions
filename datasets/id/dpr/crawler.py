from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# Members are elected from 84 electoral districts.
DAPIL_COUNT = 84


def crawl_dapil(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    dapil: int,
) -> None:
    # The site is Akamai-protected and client-rendered
    doc = zyte_api.fetch_html(
        context,
        f"{context.data_url}/{dapil}",
        unblock_validator='.//a[contains(@href, "/anggota/") and contains(@href, "/id/")]',
        geolocation="id",
        cache_days=1,
        absolute_links=True,
    )
    # A member's profile URL is "/anggota/detail/id/<id>" (or "/anggota/id/<id>"),
    # linked from both the photo (no link text) and the name, so key the members
    # on the numeric profile id, keeping the named link.
    members: dict[str, tuple[str, str]] = {}
    for link in h.xpath_elements(
        doc, '//a[contains(@href, "/anggota/") and contains(@href, "/id/")]'
    ):
        href = link.get("href")
        if href is None:
            continue
        member_id = href.rsplit("/id/", 1)[-1]
        if not member_id.isdigit():
            continue
        name = h.element_text(link)
        if not name:
            continue
        members.setdefault(member_id, (name, href))

    for member_id, (name, href) in members.items():
        person = context.make("Person")
        person.id = context.make_slug(member_id)
        person.add("name", name)
        person.add("sourceUrl", href)
        # DPR candidates must be Indonesian citizens (Law No. 7 of 2017 on General
        # Elections, Article 240 paragraph (1)). https://peraturan.bpk.go.id/Details/37644
        person.add("citizenship", "id")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            occupancy.add("constituency", f"Dapil {dapil}")
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's Representative Council of Indonesia",
        country="id",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328632",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for dapil in range(1, DAPIL_COUNT + 1):
        crawl_dapil(context, position, categorisation, dapil)
