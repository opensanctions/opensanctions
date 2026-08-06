from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The two member listings that together make up the Legislative Assembly.
MEMBER_SECTIONS = ("peoples-representatives", "nobles-representatives")


def crawl_section(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    section: str,
) -> None:
    url = f"https://www.parliament.gov.to/en/members/{section}"
    doc = context.fetch_html(url, cache_days=1)
    members: dict[str, str] = {}
    for link in h.xpath_elements(doc, f'//a[contains(@href, "/members/{section}/")]'):
        href = link.get("href")
        assert href is not None
        slug = href.rstrip("/").split("/")[-1]
        if not slug or slug == section:
            continue
        members[slug] = h.element_text(link)

    for slug, raw_name in members.items():
        assert raw_name, f"Empty member name for {slug!r}"
        # Nobles are listed by their noble title ("Lord ...", "Prince ..."), which is
        # their identifier and is kept; only pure honorifics are configured for
        # stripping.
        name = h.strip_name_titles(context, raw_name)
        if name is None:
            continue
        person = context.make("Person")
        person.id = context.make_slug(slug)
        person.add("name", name, original_value=raw_name if name != raw_name else None)
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


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Tonga",
        country="to",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328621",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for section in MEMBER_SECTIONS:
        crawl_section(context, position, categorisation, section)
