import re

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise

# The biography opens with a "Born ..." sentence carrying the date of birth. The
# wording varies ("Born ...", "Born on ...", "Born in Minsk June 20, 1962."),
# so we anchor on the sentence and capture the date-shaped token anywhere within it.
BORN_DATE = re.compile(r"^Born\b[^\n]*?\b([A-Z][a-z]+ \d{1,2}, \d{4})\b", re.MULTILINE)


def crawl_member(
    context: Context,
    source_url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    detail_page = context.fetch_html(source_url)
    name = h.xpath_string(detail_page, ".//h1/text()").strip()

    person = context.make("Person")
    person.id = context.make_id(name, source_url)
    person.add("name", name, lang="eng")
    person.add("sourceUrl", source_url)
    # Members of the Council of the Republic must be citizens of Belarus (Constitution
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

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of the Republic of Belarus",
        country="by",
        wikidata_id="Q15623433",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    for member_link in h.xpath_strings(
        doc, '//a[contains(@href, "/senators-en/view/")]/@href'
    ):
        crawl_member(context, member_link, position, categorisation)
