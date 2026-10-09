from typing import cast
import re
from datetime import date, timedelta
from urllib.parse import urlencode

from zavod import Context
from zavod import helpers as h
from zavod.util import Element

SUBTITLE_PATTERN = re.compile(
    r"""
^(Sanctions?|Décisions?|amende)\s(administratives?)\s
# "de"/"du" are elided to "d’" before a vowel, and then carry no trailing space.
((prononcées?\s)?à\sl’encontre\s(d[eu]\s|d’)|imposée\sà\s)
(
    gestionnaire\sde\sfonds\sd’investissement\salternatifs?|
    gestionnaire\sde\sfonds\sd’investissement|
    l’entreprise\sd’investissement|
    l’établissement\sde\spaiement|
    professionnel\sdu\ssecteur\sfinancier|
    l’établissement\sde\scrédit|
    cabinet\sde\srévision\sagréé|
    la\ssociété\sd’investissement\sà\scapital\svariable|
    la\ssociété|
    gestionnaire\sdu\sfonds\sd’investissement
)?
\s*
""",
    re.IGNORECASE | re.VERBOSE,
)

LISTING_URL = "https://www.cssf.lu/fr/publications-donnees/"
PAGE_SIZE = 20
MAX_PASSES = 5


def crawl_item(context: Context, card: Element) -> None:
    # The title is in the format "Sanction administrative du XX XXXX 20XX"
    title = h.xpath_element(card, ".//*[@class='library-element__title']")
    detail_url = h.xpath_string(title, ".//a/@href")
    date = " ".join(h.element_text(title).split(" ")[-3:]).replace("1er", "1")
    subtitle_el = h.xpath_element(card, ".//*[@class='library-element__subtitle']")
    subtitle = h.element_text(subtitle_el)
    stripped_subtitle = SUBTITLE_PATTERN.sub("", subtitle, count=1)

    # Check if it's starts with a upper case and if the pattern was removed
    if (
        stripped_subtitle[0] == stripped_subtitle[0].upper()
        and stripped_subtitle != subtitle
    ):
        names = [stripped_subtitle]
    # Else, try to find the name of the company on the subtitle
    else:
        res = context.lookup("subtitle_to_names", subtitle)
        if res:
            names = cast("list[str]", res.names)
        else:
            # Try to look up based on the detail URL
            url_to_name_res = context.lookup("url_to_names", detail_url)

            if url_to_name_res:
                names = cast("list[str]", url_to_name_res.names)
            else:
                context.log.warning(
                    "Can't find the name of the company in subtitle, skipping",
                    subtitle=subtitle,
                    url=detail_url,
                )
                return

    # If the subtitle doesn't contain any names
    if not names:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(*names)
    entity.add("name", names)
    entity.add("topics", "reg.warn")
    entity.add("sourceUrl", h.xpath_strings(subtitle_el, ".//a/@href"))

    sanction = h.make_sanction(context, entity, h.element_text(title))
    h.apply_date(sanction, "date", date)

    sanction.add("sourceUrl", detail_url)
    for href in h.xpath_strings(card, ".//a[contains(@class, 'pdf')]/@href"):
        sanction.add("sourceUrl", href)

    context.emit(entity)
    context.emit(sanction)


def fetch_listing(context: Context, start: date, end: date, page: int) -> Element:
    params = {
        "content_type": "1387,623,625",
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
    }
    url = f"{LISTING_URL}page/{page}/"
    return context.fetch_html(f"{url}?{urlencode(params)}", absolute_links=True)


def get_cards(doc: Element) -> list[Element]:
    return h.xpath_elements(doc, ".//li[@class='library-element']")


def crawl_paginated(context: Context, start: date, end: date, count: int) -> None:
    """Paginate a window that doesn't fit on one page, re-paginating until every
    item has been seen, since items sharing a date shift between pages."""
    seen: set[str] = set()
    for _ in range(MAX_PASSES):
        page = 1
        while True:
            cards = get_cards(fetch_listing(context, start, end, page))
            if not cards:
                break
            for card in cards:
                detail_url = h.xpath_string(
                    card, ".//*[@class='library-element__title']//a/@href"
                )
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                crawl_item(context, card)
            page += 1
        if len(seen) >= count:
            return
    context.log.warning(
        "Did not see all items in window after re-paginating",
        start=start.isoformat(),
        end=end.isoformat(),
        seen=len(seen),
        count=count,
    )


def crawl_window(context: Context, start: date, end: date) -> None:
    doc = fetch_listing(context, start, end, 1)
    # The count is absent when there are no results.
    count_texts = h.xpath_strings(doc, ".//*[@class='documents-count']/text()")
    count = int(count_texts[0].split()[0]) if count_texts else 0
    if count == 0:
        return
    if count > PAGE_SIZE:
        if start < end:
            mid = start + (end - start) / 2
            context.log.info(
                "Window has more items than fit on a page, bisecting",
                start=start.isoformat(),
                end=end.isoformat(),
                count=count,
            )
            crawl_window(context, start, mid)
            crawl_window(context, mid + timedelta(days=1), end)
            return
        context.log.info(
            "More items published on one day than fit on a page, paginating",
            date=start.isoformat(),
            count=count,
        )
        crawl_paginated(context, start, end, count)
        return
    context.log.info(
        "Crawling window", start=start.isoformat(), end=end.isoformat(), count=count
    )
    for card in get_cards(doc):
        crawl_item(context, card)


def crawl(context: Context) -> None:
    # The listing is sorted by publication date, but items sharing a date come
    # back in a different order on each request. Where such a group straddles a
    # page boundary, paginating shows some items twice and skips others.
    # Instead, bisect the publication date range until every window fits on a
    # single page.
    crawl_window(context, date(2000, 1, 1), date.today())
