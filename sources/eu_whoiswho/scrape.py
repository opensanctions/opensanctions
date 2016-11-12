import logging
import sys
import json
import requests
try:
    from queue import Queue
except ImportError:
    from Queue import Queue
from threading import Thread
from urlparse import urljoin
from lxml import html


URL = 'http://europa.eu/whoiswho/public/index.cfm?fuseaction=idea.hierarchy&lang=en'
logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARN)
log = logging.getLogger('whoiswho')
queue = Queue(maxsize=0)


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

    log.info('Person: %(name)r, %(title)r', data)
    if len(hierarchy) > 1:
        data['institution'] = hierarchy[1]

    persons.append(data)
    if len(persons) % 100 == 0:
        with open(out_file, 'w') as fh:
            json.dump({'persons': persons}, fh)


def scrape_url(persons, out_file, url):
    res = requests.get(url)
    doc = html.fromstring(res.content)
    if doc.find('.//table[@id="person-detail"]') is not None:
        scrape_person(persons, out_file, doc, url)
    for link in doc.findall('.//table[@id="mainContent"]//ul//a'):
        url = urljoin(URL, link.get('href'))
        if 'fuseaction=idea.hierarchy' in url:
            queue.put(url)


def scrape_whoiswho(out_file):
    seen = set([])
    persons = []

    def consume():
        while True:
            try:
                url = queue.get(True)
                if url not in seen:
                    seen.add(url)
                    scrape_url(persons, out_file, url)
            except Exception as e:
                log.exception(e)
            queue.task_done()

    for i in range(10):
        t = Thread(target=consume)
        t.daemon = True
        t.start()

    queue.put(URL)
    queue.join()

    with open(out_file, 'w') as fh:
        json.dump({'persons': persons}, fh)

if __name__ == '__main__':
    scrape_whoiswho(sys.argv[1])
