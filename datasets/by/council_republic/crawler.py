from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The site returns HTTP 403 to the default client; a browser User-Agent is accepted.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of the Republic of Belarus",
        country="by",
        wikidata_id="Q15623433",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, headers=HEADERS, cache_days=1)
    seen: set[str] = set()
    for link in h.xpath_elements(doc, '//a[contains(@href, "/senators-en/view/")]'):
        href = link.get("href")
        assert href is not None
        slug = href.rstrip("/").split("/")[-1]
        name = h.element_text(link)
        if not name or slug in seen:
            continue
        seen.add(slug)

        person = context.make("Person")
        person.id = context.make_slug(slug)
        person.add("name", name, lang="eng")
        person.add("sourceUrl", urljoin(context.data_url, href))
        # Members of the Council of the Republic must be citizens of Belarus (Constitution
        # of the Republic of Belarus, Article 92).
        # https://www.constituteproject.org/constitution/Belarus_2004
        person.add("citizenship", "by")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)

    if not seen:
        raise ValueError("No senators found in the Council directory")
