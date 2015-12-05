import sys
import requests
import json
from urlparse import urljoin
from lxml import html


URL = 'http://www.insolvencydirect.bis.gov.uk/IESdatabase/viewdirectorsummary-new.asp'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.73 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.8,de;q=0.6,es;q=0.4',
    'Cache-Control': 'no-cache'
}


def scrape(cache_path, out_path):
    sess = requests.Session()
    sess.headers.update(HEADERS)
    try:
        res = sess.get(URL, verify=False)
    except Exception as e:
        print dir(e), e.args
        return
    doc = html.fromstring(res.content)
    cases = []
    for link in doc.findall('.//a'):
        print link.get('href')

    with open(out_path, 'w') as fh:
        json.dump(cases, fh)


if __name__ == '__main__':
    cache_path = sys.argv[1]
    out_path = sys.argv[2]
    scrape(cache_path, out_path)
