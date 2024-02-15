from dateutil.parser import parse

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def parse_german_date(date_string):
    return parse(date_string, dayfirst=True, locale='de')

def crawl_item(url_info_page: str, context: Context):

    info_page = context.fetch_html(url_info_page)

    first_name = info_page.xpath("//span[@itemprop='http://schema.org/givenName']/text()")[0]
    last_name = info_page.xpath("//span[@itemprop='http://schema.org/familyName']/text()")[0]

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name)

    person.add("firstName", first_name)
    person.add("lastName", last_name)

    person.add("sourceUrl", url_info_page)

    birth_date_in_german = info_page.find("//span[@itemprop='birthDate']/text()")
    if birth_date_in_german:
        person.add("birthDate", parse_german_date(birth_date_in_german))

    try:
        person.add("email", info_page.xpath("//a[@itemprop='http://schema.org/email']/text()")[0])
    except:
        pass

    try:
        person.add("phone", info_page.xpath("//a[@itemprop='http://schema.org/telephone']/text()")[0]).replace(' ', '')
    except:
        pass

    # We first find the tag with the name, then go to the div that containts it, and finally find the p tag that is in bold
    position_name = info_page.xpath("//span[@itemprop='http://schema.org/givenName']/../../p[contains(@class, 'bold')]")[0]

    position = h.make_position(context, position_name, country="at")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            categorisation=categorisation,
        )

    if occupancy is None:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)
    

def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    # XPath to the url for the pages of each politician
    xpath_politician_page = '//*[contains(@class, "abgeordneter")]/*/a/@href'

    for item in response.xpath(xpath_politician_page):
        crawl_item(item, context)
