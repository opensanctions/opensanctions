import os
import sys
import json
import requests
from urlparse import urljoin
from lxml import html


URL = 'http://europa.eu/whoiswho/public/index.cfm?fuseaction=idea.hierarchy&lang=en'


def scrape_person(persons, out_file, doc, url):
    hierarchy = doc.find('.//span[@itemtype="http://data-vocabulary.org/Breadcrumb"]')
    # Remove empty items in the list
    hierarchy = [item.text_content() for item in hierarchy if item.text_content() and item.text_content().strip()]
    # Strip first item ('institution') and last item ('name of person')
    hierarchy = hierarchy[1:-1]
    data = {
        'name': doc.find('.//h3[@itemprop="name"]').text_content(),
        'title': doc.findtext('.//td[@itemprop="jobTitle"]'),
        'phone': doc.findtext('.//span[@itemprop="telephone"]'),
        'street_address': doc.findtext('.//span[@itemprop="streetAddress"]'),
        'postal_code': doc.findtext('.//span[@itemprop="postalCode"]'),
        'address_locality': doc.findtext('.//span[@itemprop="addressLocality"]'),
        'hierarchy': hierarchy,
        'url': url,
        'html': html.tostring(doc)
    }

    print [data['name'], data['title'], url]
    if len(hierarchy) > 1:
        data['institution'] = hierarchy[1]

    persons.append(data)
    if len(persons) % 100 == 0:
        with open(out_file, 'w') as fh:
            json.dump({'persons': persons}, fh)


def scrape_whoiswho(out_file):
    urls = set([URL])
    seen = set([])
    persons = []
    if os.path.isfile(out_file):
        with open(out_file, 'r') as fh:
            persons = json.load(fh)['persons']
    for person in persons:
        seen.add(person['url'])

    while True:
        if not len(urls):
            break
        url = urls.pop()
        if url in seen:
            continue
        seen.add(url)
        if 'fuseaction=idea.hierarchy' not in url:
            continue
        res = requests.get(url)
        doc = html.fromstring(res.content)
        if doc.find('.//table[@id="person-detail"]') is not None:
            scrape_person(persons, out_file, doc, url)
        for link in doc.findall('.//table[@id="mainContent"]//ul//a'):
            url = urljoin(URL, link.get('href'))
            urls.add(url)

    with open(out_file, 'w') as fh:
        json.dump({'persons': persons}, fh)

if __name__ == '__main__':
    scrape_whoiswho(sys.argv[1])
