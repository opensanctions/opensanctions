import os
from urllib.parse import urlparse
from lxml import etree

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

FORMATS = ["%d. %m %Y", "%d.%m.%Y", "%m/%Y"]
MONTHS = {
    "januar": "01",
    "februar": "02",
    "märz": "03",
    "april": "04",
    "mai": "05",
    "juni": "06",
    "juli": "07",
    "august": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "dezember": "12",
}


def parse_date_in_german(text):
    text = text.lower()
    for de, en in MONTHS.items():
        text = text.replace(de, en)
    return h.parse_date(text, FORMATS)


def parse_mandate(context, url, person, el):
    print(url)
    active_date_el = el.find('.//span[@class="aktiv"]')
    inactive_dates_el = el.find('.//span[@class="inaktiv"]')
    if active_date_el is not None:
        start_date = h.parse_date(
            active_date_el.text_content().replace("seit ", ""), FORMATS
        )
        end_date = None
        assume_current = True
        date_el = active_date_el
    elif inactive_dates_el is not None:
        start_date, end_date = inactive_dates_el.text_content().split(" - ")
        start_date = h.parse_date(start_date, FORMATS)
        end_date = h.parse_date(end_date, FORMATS)
        assume_current = False
        date_el = inactive_dates_el
    else:
        context.log.warning(
            "Can't parse date for mandate", url=url, text=el.text_content().strip()
        )
        return

    #position_name_el = date_el.getparent().getnext()
    position_name_el = el.xpath('.//div[contains(@class, "funktionsText")]')
    if position_name_el:
        position_name_el = position_name_el[0]
    else:
        # I think this is a copy of the markup for mobile
        return
    
    source_el = position_name_el.xpath('.//div[contains(@class, "source")]')[0]
    # Remove source and keep it, we'll use it later
    position_name_el.remove(source_el)
    # Add line breaks so we can split on this
    for br in position_name_el.xpath(".//br"):
        br.tail = br.tail + "\n" if br.tail else "\n"
    position_name = position_name_el.text_content().strip()
    position_parts = position_name.split("\n")
    position_name = position_parts[0]


    print(position_name)
    print("  ", start_date)
    print("  ", end_date)

    if len(position_name) > 70:
        context.log.warning(
            "Unexpectedly long position name, possibly capturing more than the position name.",
            url=url,
            name=position_name,
        )
        return


    # TODO: Make sure we strip party names from positions. e.g.
    # after comma, but watch out for position names which include commas.
    #   Vizebürgermeister von Feldkirch, FPÖ
    #   Abgeordneter zum Landtag von Vorarlberg, FPÖ
    # in parentheses
    #   Schriftführerin zum Landtag (Die Grünen) in Tirol
    #
    # Listing parties is potentially not a good idea.
    # Might have to use the fact that the party seems to be in the last bit of text after the bold bit???
    #
    # TODO: Add souorce links as occupany sourceUrl
    #   e.g. https://www.meineabgeordneten.at/storage/quellen/10021/www.untersiebenbrunn.gv.at_2020-07-15_9037.jpg
    # click on (i) to see

    position = h.make_position(context, position_name, country="at")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=assume_current,
        categorisation=categorisation,
    )

    if occupancy is not None:
        occupancy.add("description", position_parts[1:], lang="deu")
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl_item(url_info_page: str, context: Context):
    info_page = context.fetch_html(url_info_page, cache_days=1)

    first_name = info_page.findtext(".//span[@itemprop='http://schema.org/givenName']")
    last_name = info_page.findtext(".//span[@itemprop='http://schema.org/familyName']")

    person = context.make("Person")
    id = os.path.basename(urlparse(url_info_page).path)
    person.id = context.make_slug(id)

    h.apply_name(person, first_name=first_name, last_name=last_name)

    person.add("sourceUrl", url_info_page)

    birth_date_in_german = info_page.findtext(".//span[@itemprop='birthDate']")

    if birth_date_in_german:
        person.add("birthDate", parse_date_in_german(birth_date_in_german))

    person.add(
        "birthPlace", info_page.findtext(".//span[@itemprop='birthPlace']"), lang="deu"
    )

    email = info_page.findtext(".//a[@itemprop='http://schema.org/email']")

    if email:
        person.add("email", email)

    phone = info_page.findtext(".//a[@itemprop='http://schema.org/telephone']")

    if phone:
        person.add("phone", phone.replace(" ", ""))

    for row in info_page.xpath(
        '//div[@id="mandate"]//div[contains(@class, "funktionszeile")]'
    ):
        parse_mandate(context, url_info_page, person, row)


def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    # XPath to the url for the pages of each politician
    xpath_politician_page = '//*[contains(@class, "abgeordneter")]/*/a/@href'

    for item in response.xpath(xpath_politician_page):
        crawl_item(item, context)
