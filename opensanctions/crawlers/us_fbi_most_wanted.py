import math
import re

from dateutil.parser import ParserError, parse
from lxml import html
from pantomime.types import HTML

from opensanctions import helpers as h
from opensanctions.core import Context

FORMATS = ("%d/%m/%Y",)
FBI_URL = 'https://www.fbi.gov/wanted/%s/@@castle.cms.querylisting/%s?page=%s'

types = {
    'fugitives': 'f7f80a1681ac41a08266bd0920c9d9d8',
    'terrorism': '55d8265003c84ff2a7688d7acd8ebd5a',
    'bank-robbers': '2514fe8f611f47d1b2c1aa18f0f6f01b',
}


def crawl_person(context, url):
    doc = context.fetch_html(url)

    name = doc.find('.//h1[@class="documentFirstHeading"]').text_content().title()
    table = doc.find('.//table[@class="table table-striped wanted-person-description"]')

    # Detect if the table with person information exists or do not make a person
    # Because sometimes they add also groups for example the whole gru group
    if table is not None and name:
        # Add the person
        person = context.make("Person")
        person.add("topics", "crime")
        person.id = context.make_slug(name)
        person.add("sourceUrl", url)
        last_name, first_name = name.split(" ", 1)
        person.add("firstName", first_name)
        person.add("lastName", last_name)

        # Add aditional information
        rows = table.findall('.//tr')
        for item in rows:
            key, value = list(filter(str.strip, item.text_content().split("\n")))
            if "Nationality" in key and value:
                person.add("nationality", value)
            if "Place of Birth" in key and value:
                person.add("birthPlace", value)
            if "Occupation" in key and value:
                person.add("position", value)
            if "Date(s) of Birth Used" in key and value:
                first_date = ', '.join(value.split(', ')[:2])
                try:
                    parsed_date = parse(first_date).strftime('%d/%m/%Y')
                    person.add("birthDate", h.parse_date(parsed_date, FORMATS))
                except ParserError:
                    # Sometimes they add a range of dates
                    # With Approximately 1970 to 1971 or different formats
                    # And currenly we can not parse them
                    # TODO: find a way to parse them
                    pass
        context.emit(person, target=True)


def crawl_pages(context, type, amount):
    # Crawl every page
    for page in range(1, amount + 1):
        page_url = FBI_URL % (type, types.get(type), page)
        doc = context.fetch_html(page_url)
        details = doc.find('.//div[@class="query-results pat-pager"]')
        for row in details.findall('.//ul/li'):
            href = row.xpath('.//a')[0].get('href')
            crawl_person(context, href)


def crawl(context: Context):
    for type in types:
        url = FBI_URL % (type, types[type], 1)
        resource = "source_%s.html" % type
        path = context.fetch_resource(resource, url)
        context.export_resource(path, HTML, title=context.SOURCE_TITLE)
        with open(path, "r") as fh:
            doc = html.parse(fh)

        # Get total results count
        total_results = int(
            re.search(r'\d+', doc.find('//div[@class="row top-total"]').text_content()).group()
        )
        context.log.debug(
            "Total results",
            total_results=total_results,
            url=url
        )

        # Get total pages count
        total_pages = math.ceil(total_results / 40)

        crawl_pages(context, type, total_pages)
