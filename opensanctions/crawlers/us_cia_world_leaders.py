from urllib.parse import urljoin
from pprint import pprint  # noqa

from normality import collapse_spaces, stringify
from memorious.helpers import make_id

from opensanctions.models import Entity


def element_text(el):
    if el is None:
        return
    text = stringify(el.text_content())
    if text is not None:
        return collapse_spaces(text)


def parse_entity(context, url, country, component, row, updated_at):
    function = element_text(row.find('.//span[@class="title"]'))
    if function is None:
        return
    name = element_text(row.find('.//span[@class="cos_name"]'))
    if name is None:
        return

    uid = make_id(country, name, function)
    entity = Entity.create('us-cia-world-leaders', uid)
    entity.name = name
    entity.type = entity.TYPE_INDIVIDUAL
    entity.function = function
    entity.program = country
    entity.url = url
    entity.updated_at = updated_at
    nationality = entity.create_nationality()
    nationality.country = country

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def scrape_country(context, data):
    country_url = data.get('url')
    country_name = data.get('country')
    res = context.http.get(country_url)
    doc = res.html
    updated_at = doc.findtext('.//span[@id="lastUpdateDate"]')
    output = doc.find('.//div[@id="countryOutput"]')
    if output is None:
        return
    component = None
    for row in output.findall('.//li'):
        next_comp = row.findtext('./td[@class="componentName"]/strong')
        if next_comp is not None:
            component = next_comp
            continue
        parse_entity(context, country_url, country_name, component,
                     row, updated_at)


def scrape(context, data):
    url = context.params.get('url')
    res = context.http.rehash(data)
    doc = res.html

    for link in doc.findall('.//div[@id="cosAlphaList"]//a'):
        country_url = urljoin(url, link.get('href'))
        context.log.info("Crawling country: %s", link.text)
        context.emit(data={
            'url': country_url,
            'country': link.text
        })
