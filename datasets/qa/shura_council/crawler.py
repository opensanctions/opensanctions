from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def clean_name(raw: str) -> str:
    # Names are prefixed with an honorific and a slash, e.g.
    # "سعادة السيد / حسن بن عبدالله الغانم"; keep the part after the slash.
    name = " ".join(raw.split())
    if "/" in name:
        name = name.split("/")[-1]
    return name.strip()


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Shura Council of Qatar",
        country="qa",
        wikidata_id="Q21328600",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, headers=HEADERS, cache_days=1)
    cards = h.xpath_elements(doc, '//div[contains(@class, "card")][.//h3]')
    if not cards:
        raise ValueError("No member cards found on the Shura Council page")

    seen: set[str] = set()
    for card in cards:
        headings = h.xpath_elements(card, ".//h3")
        name = clean_name(h.element_text(headings[0])) if headings else ""
        if not name or name in seen:
            continue
        seen.add(name)

        person = context.make("Person")
        person.id = context.make_id(name)
        person.add("name", name, lang="ara")
        # Shura Council members must hold original Qatari nationality (Constitution of
        # Qatar, Article 80(1)). https://www.constituteproject.org/constitution/Qatar_2003
        person.add("citizenship", "qa")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
