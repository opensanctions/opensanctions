from typing import List, cast
import re

from zavod import Context
from zavod import helpers as h


SUBTITLE_PATTERN = re.compile(
    r"""
^(Sanctions?|Décisions?|amende)\s(administratives?)\s
((prononcées?\s)?à\sl’encontre\sd[eu]|imposée\sà)\s
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


def crawl_item(card, context: Context):
    # The title is in the format "Sanction administrative du XX XXXX 20XX"
    title = card.find(".//*[@class='library-element__title']")
    detail_url = title.find(".//a").get("href")
    date = " ".join(title.text_content().strip().split(" ")[-3:]).replace("1er", "1")
    subtitle_el = card.find(".//*[@class='library-element__subtitle']")
    subtitle = subtitle_el.text_content().strip()
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
            names = cast("List[str]", res.names)
        else:
            # Try to look up based on the detail URL
            url_to_name_res = context.lookup("url_to_names", detail_url)

            if url_to_name_res:
                names = cast("List[str]", url_to_name_res.names)
            else:
                context.log.warning(
                    "Can't find the name of the company in subtitle, skipping",
                    subtitle=subtitle,
                    url=title.find(".//a").get("href"),
                )
                return

    # If the subtitle doesn't contain any names
    if not names:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(*names)
    entity.add("name", names)
    entity.add("topics", "reg.warn")
    subtitle_link = subtitle_el.find(".//a")
    if subtitle_link is not None:
        entity.add("sourceUrl", subtitle_link.get("href"))

    sanction = h.make_sanction(context, entity, title.text_content().strip())
    h.apply_date(sanction, "date", date)

    sanction.add("sourceUrl", detail_url)
    for a in card.xpath(".//a[contains(@class, 'pdf')]"):
        sanction.add("sourceUrl", a.get("href"))

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    base_url = "https://www.cssf.lu/fr/publications-donnees/page/{}/?content_type=1387%2C623%2C625"

    idx = 1

    while True:
        url = base_url.format(idx)
        response = context.fetch_html(url, absolute_links=True)

        idx += 1

        # Find all cards to the sanctions
        cards = response.findall(".//li[@class='library-element']")

        if not cards:
            break

        for card in cards:
            crawl_item(card, context)
