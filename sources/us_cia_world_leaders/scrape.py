import sys
import requests
import json
from urlparse import urljoin
from lxml import html


URL = 'https://www.cia.gov/library/publications/resources/world-leaders-1/index.html'


def scrape_country(data):
    res = requests.get(data['url'])
    doc = html.fromstring(res.content)
    data['last_update'] = doc.findtext('.//span[@id="lastUpdateDate"]')
    if data['last_update']:
        data['last_update'] = data['last_update'].strip()
    output = doc.find('.//div[@id="countryOutput"]')
    if output is None:
        return
    component = None
    existing = set()
    rows = []
    for row in output.findall('.//li'):
        next_comp = row.findtext('./td[@class="componentName"]/strong')
        if next_comp is not None:
            component = next_comp
            continue
        title = row.find('.//span[@class="title"]')
        if title is None:
            continue
        title = title.text_content().strip()
        name = row.find('.//span[@class="cos_name"]')
        if name is None:
            continue
        name = name.text_content().strip()
        key = (title, name)
        if key in existing:
            continue
        existing.add(key)
        row = {
            'title': title,
            'component': component,
            'name': name
        }
        row.update(data)
        rows.append(row)
    return rows


def scrape(out_path):
    res = requests.get(URL)
    doc = html.fromstring(res.content)

    leaders = []
    for link in doc.findall('.//div[@id="cosAlphaList"]//a'):
        url = urljoin(URL, link.get('href'))
        print 'Scraping', url
        leaders.extend(scrape_country({
            'url': url,
            'country': link.text
        }))
        print 'Leaders len: ', len(leaders)
        with open(out_path, 'w') as fh:
            json.dump({'leaders': leaders}, fh, indent=2)


if __name__ == '__main__':
    scrape(sys.argv[1])
