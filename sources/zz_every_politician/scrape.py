import os
import sys
import json
import urllib
from urlparse import urljoin
import requests
# from unicodecsv import DictReader

from pprint import pprint  # noqa


INDEX = "https://raw.githubusercontent.com/everypolitician/everypolitician-data/master/countries.json"


def scrape_files(out_dir):
    res = requests.get(INDEX)
    with open(os.path.join(out_dir, 'countries.json'), 'wb') as fh:
        json.dump(res.json(), fh, indent=2)

    for country in res.json():
        legs = country.pop('legislatures', [])
        country_data = {'country_' + k: v for k, v in country.items()}
        for legislature in legs:
            periods = legislature.pop('legislative_periods', [])
            leg_data = {'legislature_' + k: v for k, v in legislature.items()}
            leg_data.update(country_data)
            for period in periods:
                source_url = urljoin(INDEX, period.get('csv'))
                scrape_csv(out_dir, source_url)


def scrape_csv(out_dir, source_url):
    local_name = source_url.split('/data/')[-1]
    local_name = os.path.join(out_dir, local_name)
    try:
        os.makedirs(os.path.dirname(local_name))
    except:
        pass
    print 'CSV: ', local_name
    urllib.urlretrieve(source_url, local_name)

    # print 'CSV: %(source_url)s' % data
    # rows = []
    # with open(local_file, 'rn') as fh:
    #     for row in DictReader(fh):
    #         row.update(data)
    #         # row['person_id'] = row.pop('id', None)
    #         # pprint(row)
    #         rows.append(row)
    # return rows

if __name__ == '__main__':
    scrape_files(sys.argv[1])
