from zavod import Context, helpers as h
from zavod.logic.pep import categorise

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

def parse_date(text):
    text = text.lower()
    for de, en in MONTHS.items():
        text = text.replace(de, en)
    return h.parse_date(text, ["%d. %m %Y"])


def crawl_item(url_info_page: str, context: Context):

    info_page = context.fetch_html(url_info_page)

    first_name = info_page.findtext(".//span[@itemprop='http://schema.org/givenName']")
    last_name = info_page.findtext(".//span[@itemprop='http://schema.org/familyName']")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name)

    h.apply_name(person, first_name=first_name, last_name=last_name)

    person.add("sourceUrl", url_info_page)

    birth_date_in_german = info_page.findtext(".//span[@itemprop='birthDate']")

    if birth_date_in_german:
        person.add("birthDate", parse_date(birth_date_in_german))
    
    person.add("birthPlace", info_page.findtext(".//span[@itemprop='birthPlace']"), lang="deu")

    email = info_page.findtext(".//a[@itemprop='http://schema.org/email']")

    if email:
        person.add("email", email)

    phone = info_page.findtext(".//a[@itemprop='http://schema.org/telephone']")
    
    if phone:
        person.add("phone", phone.replace(' ', ''))

    # We first find the tag with the name, then go to the div that containts it, and finally find the p tag that is in bold
    position_name = info_page.xpath("//span[@itemprop='http://schema.org/givenName']/../../p[contains(@class, 'bold')]/text()")[0]

    position = h.make_position(context, position_name, country="at")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            categorisation=categorisation,
        )

    if occupancy is not None:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)
    

def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    # XPath to the url for the pages of each politician
    xpath_politician_page = '//*[contains(@class, "abgeordneter")]/*/a/@href'

    for item in response.xpath(xpath_politician_page):
        crawl_item(item, context)
