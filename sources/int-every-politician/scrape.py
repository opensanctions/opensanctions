import sys
import json
import urllib
from urlparse import urljoin
from tempfile import mkstemp
import requests
from unicodecsv import DictReader

from pprint import pprint  # noqa


INDEX = "https://raw.githubusercontent.com/everypolitician/everypolitician-data/master/countries.json"


def scrape_files(out_file):
    res = requests.get(INDEX)
    politicians = []
    for country in res.json():
        legs = country.pop('legislatures', [])
        country_data = {'country_' + k: v for k, v in country.items()}
        for legislature in legs:
            periods = legislature.pop('legislative_periods', [])
            leg_data = {'legislature_' + k: v for k, v in legislature.items()}
            leg_data.update(country_data)
            for period in periods:
                data = {'period_' + k: v for k, v in period.items()}
                data.update(leg_data)
                data['source_url'] = urljoin(INDEX, data.get('period_csv'))
                politicians.extend(scrape_csv(data))

    with open(out_file, 'wb') as fh:
        json.dump({'politicians': politicians}, fh, indent=2)


def scrape_csv(data):
    _, local_file = mkstemp()
    urllib.urlretrieve(data.get('source_url'), local_file)
    print 'CSV: %(source_url)s' % data
    rows = []
    with open(local_file, 'rn') as fh:
        for row in DictReader(fh):
            row.update(data)
            # row['person_id'] = row.pop('id', None)
            # pprint(row)
            rows.append(row)
    return rows

if __name__ == '__main__':
    scrape_files(sys.argv[1])
