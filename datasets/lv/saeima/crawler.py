from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise

TOPICS = ["gov.national", "gov.legislative"]


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    period_start: str,
    period_end: str | None,
) -> None:
    doc = context.fetch_html(url, encoding="utf-8")
    for card in h.xpath_elements(doc, "//ul[@data-view-panel='saraksts']/li"):
        href = h.xpath_string(card, ".//a[@class='dep-card__name']/@href")
        # Profile addresses end in the term and the deputy's name, numbered or located
        # for namesakes: 14-ainars-latkovskis, 9-andris-berzins-2. Without the term,
        # this is the name slug the previous source (titania.saeima.lv) was keyed on.
        slug = href.rstrip("/").rsplit("/", 1)[-1].split("-", 1)[1]

        person = context.make("Person")
        person.id = context.make_slug(slug)
        h.apply_name(
            person,
            first_name=card.get("data-vards"),
            # Namesakes carry their constituency, e.g. "Bērziņš (Rīga)".
            last_name=h.remove_bracketed(card.get("data-uzvards")),
            lang="lav",
        )
        # citizenship required (Art 9): https://likumi.lv/ta/en/id/57980-the-constitution-of-the-republic-of-latvia
        person.add("citizenship", "lv")
        person.add("sourceUrl", urljoin(url, href))

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            period_start=period_start,
            period_end=period_end,
            # Deputies who lay down their mandate to serve as ministers stay listed
            # in the sitting term as "previous", without a date.
            no_end_implies_current=card.get("data-status") == "current",
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        faction = card.get("data-frakcija-label")
        # "Deputies not belonging to a faction"
        if faction != "Pie frakcijām nepiederošie deputāti":
            occupancy.add("politicalGroup", faction, lang="lav")
        occupancy.add("constituency", card.get("data-apgabals"), lang="lav")
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        "Member of the Saeima of Latvia",
        country="lv",
        topics=TOPICS,
        wikidata_id="Q21191589",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    cutoff = h.earliest_term_start(TOPICS)
    # The site serves "text/html" without a charset.
    doc = context.fetch_html(context.data_url, encoding="utf-8")
    for term in h.xpath_elements(doc, "//div[header[@class='sas-card__head']]"):
        # "01.11.2022.-03.11.2026.", or "03.11.2026.-šobrīd" ("until now").
        dates = h.element_text(h.xpath_element(term, ".//p[@class='sas-card__dates']"))
        period_start, end = dates.split("-")
        period_end = None if end == "šobrīd" else end
        if period_end is not None and h.extract_date(context.dataset, end)[0] < cutoff:
            context.log.info("Term predates the PEP window; skipping", dates=dates)
            continue
        href = h.xpath_string(
            term, ".//a[@class='sas-card__link'][contains(@href, '/deputati/')]/@href"
        )
        crawl_term(
            context,
            position,
            categorisation,
            urljoin(context.data_url, href),
            period_start,
            period_end,
        )
