from urllib.parse import unquote

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The Lao word for "electoral constituency" appears in each per-constituency page URL.
CONSTITUENCY_MARKER = "ເຂດເລືອກຕັ້ງ"

# Honorific and title tokens prefixing a deputy's name: Mr / Mrs / Professor / Doctor /
# Associate Professor and common rank abbreviations. Stripped so the name is the actual
# personal name.
TITLE_TOKENS = {
    "ທ່ານ",  # Mr
    "ນາງ",  # Mrs / Ms
    "ສຈ.",  # Professor
    "ສຈ",
    "ຮສ.",  # Associate Professor
    "ຮສ",
    "ປອ.",  # Doctor
    "ປອ",
    "ພັທ.",
    "ພັນ",
}


def clean_name(raw: str) -> str:
    tokens = raw.split()
    # Drop leading honorifics and any rank/academic abbreviation (which always ends in a
    # full stop, e.g. "ປອ.", "ພົຕ."). Lao personal-name tokens never end in a full stop.
    while tokens and (tokens[0] in TITLE_TOKENS or tokens[0].endswith(".")):
        tokens.pop(0)
    # A rank abbreviation is sometimes glued to the given name, e.g. "ພັອ.ຄຳດອນ"; keep
    # only the part after the final full stop.
    if tokens and "." in tokens[0]:
        tokens[0] = tokens[0].rsplit(".", 1)[-1]
    return " ".join(tokens).strip()


def crawl_constituency(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
) -> int:
    doc = context.fetch_html(url, cache_days=7)
    count = 0
    for heading in h.xpath_elements(doc, '//h2[starts-with(normalize-space(), "ທ່ານ")]'):
        name = clean_name(h.element_text(heading))
        if not name:
            continue
        person = context.make("Person")
        person.id = context.make_id(name, url)
        person.add("name", name, lang="lao")
        person.add("sourceUrl", url)
        # The right to stand for election is reserved to Lao citizens (Constitution of the
        # Lao PDR, Article 36). https://www.constituteproject.org/constitution/Laos_2015
        person.add("citizenship", "la")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
        count += 1
    return count


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Laos",
        country="la",
        wikidata_id="Q21295987",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    urls: list[str] = []
    seen: set[str] = set()
    for href in h.xpath_strings(doc, "//a/@href"):
        if CONSTITUENCY_MARKER in unquote(href) and href not in seen:
            seen.add(href)
            urls.append(href)
    if not urls:
        raise ValueError("No constituency links found on the members hub")

    total = 0
    for url in urls:
        total += crawl_constituency(context, position, categorisation, url)
    if total == 0:
        raise ValueError("No deputies found across constituency pages")
