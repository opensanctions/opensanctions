import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Constituency links on the members hub are numbered "1. Constituency 1, Vientiane
# Capital" through "18. Constituency 18, Xaysomboun Province". Matching on the link
# text (rather than a URL keyword) also catches constituency 1, whose link is the hub
# page itself, and constituency 2, whose URL slug ("phongsaly2") is not in Lao script.
REGEX_CONSTITUENCY_LINK = re.compile(r"^\d{1,2}\. Constituency \d{1,2},")
# One constituency per province, plus the Vientiane Capital prefecture.
CONSTITUENCY_COUNT = 18


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    lao_raw: str,
    eng_raw: str,
) -> None:
    person = context.make("Person")
    person.id = context.make_id(lao_raw, url)
    for raw_name, lang in ((lao_raw, "lao"), (eng_raw, "eng")):
        name = h.strip_name_titles(context, raw_name)

        # A full stop left after title stripping (flagged via `names.schema_rules.
        # Person.reject_chars`) means a rank/academic abbreviation missing from
        # `names.prefixes_strip` — unless it is a known surname initial, cleared
        # in the `names_override` lookup.
        if (
            h.is_name_irregular(person, name)
            and context.lookup_value("names_override", name) is None
        ):
            context.log.warning(
                "Name still looks titled after stripping; extend names.prefixes_strip",
                name=name,
                raw_name=raw_name,
            )
        person.add(
            "name",
            name,
            lang=lang,
            original_value=raw_name if name != raw_name else None,
        )

    person.add("sourceUrl", url)
    # The right to stand for election is reserved to Lao citizens (Constitution of the
    # Lao PDR, Article 36). https://www.constituteproject.org/constitution/Laos_2015
    person.add("citizenship", "la")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl_constituency(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
) -> None:
    doc = context.fetch_html(url, cache_days=7)
    # Each deputy is a block holding the Lao name in an <h2> and its English
    # counterpart in an <h3>.
    members = h.xpath_elements(doc, '//div[contains(@class, "naperson")]')
    for member in members:
        lao_raw = h.element_text(h.xpath_element(member, ".//h2"))
        eng_raw = h.element_text(h.xpath_element(member, ".//h3"))
        crawl_member(context, position, categorisation, url, lao_raw, eng_raw)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Laos",
        country="la",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295987",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in h.xpath_elements(doc, "//a[@href]"):
        if REGEX_CONSTITUENCY_LINK.match(h.element_text(anchor)) is None:
            continue
        href = anchor.get("href")
        if href is None or href in seen:
            continue
        seen.add(href)
        urls.append(href)
    if len(urls) != CONSTITUENCY_COUNT:
        raise ValueError(
            f"Expected {CONSTITUENCY_COUNT} constituency links, found {len(urls)}"
        )

    for url in urls:
        crawl_constituency(context, position, categorisation, url)
