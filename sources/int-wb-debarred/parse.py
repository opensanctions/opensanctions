import re
import sys
import logging
from hashlib import sha1

from lxml import html
from dateutil.parser import parse as dateutil_parse

from peplib import Source
from peplib.util import make_id
from peplib.country import normalize_country

log = logging.getLogger(__name__)
source = Source('wb-debarred')

SPLITS = r'(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)'

SOURCE = {
    'publisher': 'World Bank',
    'publisher_url': 'http://www.worldbank.org/',
    'source': 'Debarred & Cross-Debarred Firms & Individuals',
    'source_url': 'http://web.worldbank.org/external/default/main?contentMDK=64069844&menuPK=116730&pagePK=64148989&piPK=64148984&querycontentMDK=64069700&theSitePK=84266',
    'type': 'entity'
}


def clean_value(el):
    text = el.text_content().strip()
    text = text.replace(u',\xa0\n\n', ' ')
    return text.strip()


def clean_name(text):
    text = text.replace('M/S', 'MS')
    parts = re.split(SPLITS, text, re.I)
    names = []
    keep = True
    for part in parts:
        if part is None:
            continue
        if keep:
            names.append(part)
            keep = False
        else:
            keep = True

    clean_names = []
    for name in names:
        name, _ = name.rsplit('*', 1)
        # name = re.sub(r'\* *\d{1,4}$', '', name)
        name = name.strip(')').strip('(').strip(',')
        name = name.strip()
        clean_names.append(name)
    return clean_names


def wbdeb_parse(html_file):
    doc = html.parse(html_file)
    for table in doc.findall('//table'):
        if 'List of Debarred' not in table.get('summary', ''):
            continue
        rows = table.findall('.//tr')
        print table.get('summary'), len(rows)
        for row in rows:
            tds = row.findall('./td')
            if len(tds) != 6:
                continue
            values = [clean_value(td) for td in tds]
            uid = sha1()
            for value in values:
                uid.update(value.encode('utf-8'))
            uid = uid.hexdigest()[:10]

            names = clean_name(values[0])
            if not len(names):
                log.warning("No name: %r", values)
                continue

            record = {
                'uid': make_id('wb', 'debarred', uid),
                'name': names[0],
                'nationality': normalize_country(values[2]),
                'program': values[5],
                'addresses': [{
                    'text': values[1],
                    'country': normalize_country(values[2])
                }],
                'other_names': [],
                'updated_at': dateutil_parse(values[3]).date().isoformat()
            }

            for name in names[1:]:
                record['other_names'].append({
                    'other_name': name
                })
            record.update(SOURCE)
            source.emit(record)


if __name__ == '__main__':
    wbdeb_parse(sys.argv[1])
