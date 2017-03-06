import sys
import json
import requests
from datetime import datetime
from urlparse import urljoin
from lxml import html
from normality import slugify
from itertools import count


def scrape_case(url):
    res = requests.get(url)
    # empty = 'class="nom_fugitif_wanted">Identity unknown</div>' not in res.content
    # if empty:
    #     print "MISSING", url
    #    return
    doc = html.fromstring(res.content)
    data = {
        'url': url,
        'last_updated': datetime.utcnow().isoformat(),
        'name': doc.find('.//div[@class="nom_fugitif_wanted"]').text_content(),
        'reason': doc.find('.//span[@class="nom_fugitif_wanted_small"]').text_content(),
        # 'html': res.content
    }
    for row in doc.findall('.//div[@class="bloc_detail"]//tr'):
        title, value = row.findall('./td')
        name = slugify(title.text_content(), sep='_')
        if name is None:
            continue
        if len(name):
            data[name] = value.text_content().strip()

    print 'Wanted: %s' % data['name'].encode('utf-8')
    return data


def scrape(out_file):
    url = 'http://www.interpol.int/notice/search/wanted/(offset)/%s'
    cases = []
    print 'Storing to JSON: %s' % out_file
    for i in count(0):
        p = i * 9
        res = requests.get(url % p)
        doc = html.fromstring(res.content)
        links = doc.findall('.//div[@class="wanted"]//a')
        if not len(links):
            break
        for link in links:
            case_url = urljoin(url, link.get('href'))
            cases.append(scrape_case(case_url))

    with open(out_file, 'w') as fh:
        json.dump({'cases': cases}, fh, indent=2)


if __name__ == '__main__':
    scrape(sys.argv[1])
