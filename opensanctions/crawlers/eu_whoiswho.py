from urllib.parse import urljoin
from pprint import pprint  # noqa

from memorious.helpers import make_id

from opensanctions.models import Entity


URL = 'http://europa.eu/whoiswho/public/index.cfm?fuseaction=idea.hierarchy&lang=en'  # noqa


def scrape_person(context, doc, url):
    hierarchy = doc.find(
        './/span[@itemtype="http://data-vocabulary.org/Breadcrumb"]')
    # Remove empty items in the list
    hierarchy = [
        item.text_content() for item in hierarchy
        if item.text_content() and item.text_content().strip()
    ]
    # Strip first item ('institution') and last item ('name of person')
    hierarchy = hierarchy[1:-1]

    name = doc.find('.//h3[@itemprop="name"]').text_content()
    title = doc.findtext('.//td[@itemprop="jobTitle"]')
    entity_id = make_id(name, title)
    entity = Entity.create('eu-whoiswho', entity_id)
    entity.name = name
    entity.url = url
    entity.function = title

    address = entity.create_address()
    address.street = doc.findtext('.//span[@itemprop="streetAddress"]')
    address.postal_code = doc.findtext('.//span[@itemprop="postalCode"]')
    address.text = doc.findtext('.//span[@itemprop="addressLocality"]')
    # address.phone = doc.findtext('.//span[@itemprop="telephone"]')

    if len(hierarchy) > 1:
        entity.program = hierarchy[1]

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def scrape_url(context, url, seen):
    res = context.http.get(url)
    doc = res.html
    if doc.find('.//table[@id="person-detail"]') is not None:
        scrape_person(context, doc, url)
    for link in doc.findall('.//table[@id="mainContent"]//ul//a'):
        url = urljoin(URL, link.get('href'))
        if 'fuseaction=idea.hierarchy' in url and url not in seen:
            seen.add(url)
            scrape_url(context, url, seen)


def scrape(context, data):
    seen = set([])
    url = URL
    try:
        seen.add(url)
        scrape_url(context, url, seen)
    except Exception as e:
        context.log.warning(e)
