import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


# Each deputy links to a detail page "/<id>-<slug>", e.g. "/1302-guend-nabil". This is
# the stable anchor we key on; the slug carries the transliterated name.
DEPUTY_HREF_RE = re.compile(r"^/?(\d+)-([a-z][a-z0-9-]+)$")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    link: Element,
) -> None:
    match = DEPUTY_HREF_RE.match(h.xpath_string(link, "./@href"))
    if match is None:
        return
    deputy_id, slug = match.group(1), match.group(2)

    # Prefer the rendered link text (native spelling); fall back to the slug.
    name = h.element_text(link) or slug.replace("-", " ").title()

    person = context.make("Person")
    # Deputies linked more than once on the page merge on this deterministic ID.
    person.id = context.make_slug(deputy_id)
    person.add("name", name)
    person.add("sourceUrl", f"https://www.apn.dz/{deputy_id}-{slug}")
    # A candidate for the APN must be of Algerian nationality (Organic Law 21-01 on
    # the electoral regime, Article 200; 2020 Constitution Article 128).
    # https://cour-constitutionnelle.dz/wp-content/uploads/2023/02/loi%20-electFR.pdf
    person.add("citizenship", "dz")

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
        name="Member of the People's National Assembly of Algeria",
        country="dz",
        wikidata_id="Q21290886",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator="//a[@href]",
        geolocation="dz",
        cache_days=14,
    )

    for link in h.xpath_elements(doc, "//a[@href]"):
        crawl_member(context, position, categorisation, link)
