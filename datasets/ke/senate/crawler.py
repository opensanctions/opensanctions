# import re
from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# # Leading honorifics/titles to strip from listing names; post-nominals (CBS, MP, OGW…)
# # sit after a comma and are dropped with everything past the first comma.
# TITLES = {
#     "sen",
#     "hon",
#     "dr",
#     "prof",
#     "amb",
#     "eng",
#     "justice",
#     "rtd",
#     "gen",
#     "capt",
#     "maj",
#     "col",
#     "cs",
#     "bishop",
#     "rev",
#     "mrs",
#     "mr",
#     "ms",
# }
#
#
# def clean_name(raw: str) -> str:
#     name = re.sub(r"\([^)]*\)", " ", raw)  # drop "(Dr.)", "(Rtd)" etc.
#     name = name.split(",")[0]  # drop trailing post-nominals
#     tokens = name.split()
#     while tokens and tokens[0].lower().strip(".") in TITLES:
#         tokens.pop(0)
#     return " ".join(tokens)


def field(row: HtmlElement, name: str) -> str | None:
    cells = h.xpath_elements(row, f".//td[contains(@class, '{name}')]")
    return h.element_text(cells[0]) if cells else None


def crawl_senator(
    context: Context,
    row: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    raw_name = field(row, "views-field-field-senator")
    if raw_name is None:
        return
    links = h.xpath_elements(row, ".//td[contains(@class, 'view-node')]//a")
    href = links[0].get("href") if links else None
    slug = href.rstrip("/").split("/")[-1] if href is not None else raw_name

    person = context.make("Person")
    person.id = context.make_slug("senator", slug)
    h.apply_name(person, full=raw_name, lang="eng")
    party = field(row, "views-field-field-party-senator")
    person.add("political", party)
    # Senators must be Kenyan citizens (Constitution art. 99); as State officers they
    # may not hold dual citizenship (art. 78(2)).
    # https://www.constituteproject.org/constitution/Kenya_2010
    person.add("citizenship", "ke")

    county = field(row, "views-field-field-county-senator")
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", county)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Kenya",
        country="ke",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18555726",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for page in count(0):
        doc = context.fetch_html(
            context.data_url, params={"page": page}, cache_days=1, absolute_links=True
        )
        rows = h.xpath_elements(
            doc, "//tr[td[contains(@class, 'views-field-field-senator')]]"
        )
        if len(rows) == 0:
            break
        for row in rows:
            crawl_senator(context, row, position, categorisation)
