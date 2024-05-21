from typing import cast

from zavod import Context, helpers as h
from datetime import datetime

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


def parse_date(date: str) -> str:
    for french_month in FRENCH_TO_ENGLISH_MONTHS:
        date = date.replace(french_month, FRENCH_TO_ENGLISH_MONTHS[french_month])

    # Replacing 1er with 1
    date = date.replace("1er", "1")

    try:
        return datetime.strptime(date, "%d %B %Y")
    except:
        return None


def crawl_item(url: str, context: Context):
    response = context.fetch_html(url)

    # The title is in the format "Sanction administrative du XX XXXX 20XX"
    title = response.find('.//*[@class="single-news__title"]')
    date = ' '.join(title.text_content().strip().split(" ")[-3:])
    subtitle = (
        response.find('.//*[@class="single-news__subtitle"]').text_content().strip()
    )
    res = context.lookup("subtitle_to_names", subtitle)
    if res:
        names = cast("List[str]", res.names)
    else:
        context.log.warning("Can't find the name of the company", text=subtitle)
        names = [subtitle]

    # If the subtitle doesn't contain any names
    if not names:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(*names)
    entity.add("name", names)

    entity.add("topics", "crime.fin")

    sanction = context.make("Sanction")
    sanction.id = context.make_slug(title)
    sanction.add("date", parse_date(date))

    for a in response.findall('.//*[@class="doc-link-title"]'):
        sanction.add("sourceUrl", a.get("href"))

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):

    base_url = "https://www.cssf.lu/fr/publications-donnees/page/{}/?content_type=1387%2C623%2C625"

    idx = 1

    while True:
        response = context.fetch_html(base_url.format(idx))

        idx += 1

        # Find all links to the sanctions
        a_tags = response.findall('.//*[@class="library-element__title"]/a')

        if not a_tags:
            break

        for a in a_tags:
            crawl_item(a.get("href"), context)
