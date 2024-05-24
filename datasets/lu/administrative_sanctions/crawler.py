from typing import List, cast
import re
from datetime import datetime

from zavod import Context
from zavod import helpers as h

FRENCH_TO_ENGLISH_MONTHS = {
    "janvier": "january",
    "février": "february",
    "mars": "march",
    "avril": "april",
    "mai": "may",
    "juin": "june",
    "juillet": "july",
    "août": "august",
    "septembre": "september",
    "octobre": "october",
    "novembre": "november",
    "décembre": "december",
}

SUBTITLE_PATTERN = re.compile(
    r"^(Sanction|Sanctions|amende) (administrative|administratives) ((prononcée|prononcées) à l’encontre d[eu]|imposée à) (gestionnaire de fonds d’investissement alternatifs|gestionnaire de fonds d’investissement|l’entreprise d’investissement|l’établissement de paiement|professionnel du secteur financier|l’établissement de crédit|cabinet de révision agréé|la société d’investissement à capital variable|gestionnaire du fonds d’investissement)?\s*",
    re.IGNORECASE,
)


def parse_date(date: str) -> str:
    for french_month in FRENCH_TO_ENGLISH_MONTHS:
        date = date.replace(french_month, FRENCH_TO_ENGLISH_MONTHS[french_month])

    # Replacing 1er with 1
    date = date.replace("1er", "1")

    try:
        return datetime.strptime(date, "%d %B %Y")
    except:
        return None


def crawl_item(card, context: Context):
    # The title is in the format "Sanction administrative du XX XXXX 20XX"
    title = card.find(".//*[@class='library-element__title']")
    date = " ".join(title.text_content().strip().split(" ")[-3:])
    subtitle = (
        card.find(".//*[@class='library-element__subtitle']").text_content().strip()
    )
    detail_url = title.find(".//a").get("href")
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
                    "Can't find the name of the company",
                    text=title.find(".//a").get("href"),
                )
                names = [subtitle]

    # If the subtitle doesn't contain any names
    if not names:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(*names)
    entity.add("name", names)
    entity.add("topics", "reg.warn")

    sanction = h.make_sanction(context, entity, title.text_content().strip())
    sanction.add("date", parse_date(date))

    sanction.add("sourceUrl", detail_url)
    for a in card.xpath(".//a[contains(@class, 'pdf')]"):
        sanction.add("sourceUrl", a.get("href"))

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    base_url = "https://www.cssf.lu/fr/publications-donnees/page/{}/?content_type=1387%2C623%2C625"

    idx = 1

    while True:
        response = context.fetch_html(base_url.format(idx))

        idx += 1

        # Find all cards to the sanctions
        cards = response.findall(".//li[@class='library-element']")

        if not cards:
            break

        for card in cards:
            crawl_item(card, context)
