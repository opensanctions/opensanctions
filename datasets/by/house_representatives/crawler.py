import re

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise


BORN_DATE = re.compile(r"^Born\b[^\n]*?\b([A-Z][a-z]+ \d{1,2}, \d{4})\b", re.MULTILINE)


def crawl_member(
    context: Context,
    member_link: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name_xpath = './/div[contains(@class, "dep_info")]/h1/text()'
    detail_page = zyte_api.fetch_html(
        context,
        member_link,
        unblock_validator=name_xpath,
        geolocation="by",
        cache_days=1,
        absolute_links=True,
    )
    name = h.xpath_string(detail_page, name_xpath).strip()

    person = context.make("Person")
    person.id = context.make_id(name, member_link)
    person.add("name", name, lang="eng")
    person.add("sourceUrl", member_link)
    # Members of the House of Representatives must be citizens of Belarus (Constitution
    # of the Republic of Belarus, Article 92).
    # https://www.constituteproject.org/constitution/Belarus_2004
    person.add("citizenship", "by")

    paragraphs = [
        p.strip()
        for p in h.xpath_strings(detail_page, '//div[@id="biography_info"]//p/text()')
        if p.strip()
    ]
    if len(paragraphs) != 0:
        biography = "\n".join(paragraphs)
        person.add("notes", biography, lang="eng")

        matches = BORN_DATE.findall(biography)
        if len(matches) == 1:
            h.apply_date(person, "birthDate", matches[0])

    constituencies = [
        c.strip()
        for c in h.xpath_strings(
            detail_page,
            './/div[contains(@class, "dep_info")]'
            '/b[contains(text(), "Constituency")]/following-sibling::text()[1]',
        )
        if c.strip()
    ]
    constituency: str | None = None
    if len(constituencies) == 1:
        constituency = constituencies[0]

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    if constituency is not None:
        occupancy.add("constituency", constituency)
    context.emit(occupancy)

    # Committee memberships and leadership roles
    roles = [
        b.strip()
        for b in h.xpath_strings(
            detail_page, './/div[contains(@class, "dep_info")]//b/text()'
        )
        if b.strip() and not b.strip().startswith("Constituency")
    ]
    for role in roles:
        role_position = h.make_position(context, name=role, country="by", lang="eng")
        role_categorisation = categorise(context, role_position)
        role_occupancy = h.make_occupancy(
            context,
            person,
            role_position,
            categorisation=role_categorisation,
        )
        if role_occupancy is None:
            continue
        context.emit(role_position)
        context.emit(role_occupancy)

    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Belarus",
        country="by",
        wikidata_id="Q14335901",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    xpath = './/a[contains(@href, "/deputies-en/view/")]/@href'
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=xpath,
        geolocation="by",
        cache_days=1,
        absolute_links=True,
    )
    for member_link in h.xpath_strings(doc, xpath):
        crawl_member(context, member_link, position, categorisation)
