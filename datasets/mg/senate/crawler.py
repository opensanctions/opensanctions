from normality import ascii_text

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


def read_label(lines: list[str], label: str) -> str | None:
    """Read the value of one `Label : value` line of a senator's card.

    A card mixes labelled fields with free-text lines naming the offices its senator
    holds inside the chamber, some of which carry a colon of their own. The labels
    vary in accents and spacing between records, so they are compared asciified and
    lowercased.
    """
    for line in lines:
        prefix, separator, value = line.partition(":")
        if separator and (ascii_text(prefix) or "").strip().lower() == label:
            return value.strip()
    return None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    card: Element,
) -> None:
    name = h.element_text(
        h.xpath_element(card, ".//h4[contains(@class, 'card-title')]")
    )
    url = h.xpath_string(card, ".//a/@href")
    if url == "#":
        # A blank template card sits among the senators, linking nowhere.
        assert name == "Sénateur", name
        return
    # The card's children are only <br> elements, so its direct text nodes are its
    # lines.
    lines = h.xpath_strings(card, ".//p[contains(@class, 'card-text')]/text()")

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("sourceUrl", url)
    person.add("political", read_label(lines, "au titre du parti"))
    # "élu"/"désigné" says whether the seat was elected or filled by presidential
    # appointment, which has no FollowTheMoney property of its own. Its feminine form
    # is the only place the source states a senator's gender.
    mandate = read_label(lines, "elu/designe")
    person.add("gender", context.lookup_value("gender", mandate, warn_unmatched=True))
    # Senators must be Malagasy citizens, whether elected or appointed: loi organique
    # n° 2015-007 du 3 mars 2015 art. 61, "Tout citoyen qui a la qualité d'électeur peut
    # être élu ou désigné sénateur ... 1. être de nationalité malagasy".
    # https://www.ceni-madagascar.mg/wp-content/uploads/2020/10/Loi-organique-n°2015-007-SENAT.pdf
    person.add("citizenship", "mg")
    # The card states an age rather than a date of birth, and the page's own older
    # copies of the roster give different ages for the same people, so it pins down no
    # birth date. The one death the roster has not caught up with is held in a lookup.
    h.apply_date(person, "deathDate", context.lookup_value("death_date", name))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # The roster dates no one, so each occupancy carries the legislature's bounds:
        # the senators elected on 2020-12-11 took office at the special session that
        # opened on 2021-01-19, and the Haute Cour Constitutionnelle held in its avis
        # of 2026-01-19 that their five-year mandate had expired "sans prorogation
        # possible" at midnight on 2026-01-18.
        # https://senat.mg/historique-v2/ and https://www.hcc.gov.mg/?p=9804
        period_start="2021-01-19",
        period_end="2026-01-18",
    )
    if occupancy is None:
        return
    # Each province returns two senators through an electoral college of its mayors and
    # municipal councillors. The card names a province for the appointed senators too.
    occupancy.add("constituency", read_label(lines, "province"))
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Madagascar",
        wikidata_id="Q21295145",
        country="mg",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    # The page shows the roster three times over: the tab read here, and two older
    # copies of it under the Bureau Permanent and Conférence des Présidents tabs, which
    # disagree with it on ages and offices and send one senator's link to another's
    # page.
    tab = h.xpath_element(doc, ".//div[@id='tab1']")
    cards = h.xpath_elements(tab, ".//div[@class='card text-center']")
    # The roster has stood unchanged since the chamber emptied, which is what lets the
    # occupancies above be dated from the expired term. Stop rather than date a newly
    # seated chamber from it.
    assert len(cards) == 20, len(cards)
    for card in cards:
        crawl_member(context, position, categorisation, card)
