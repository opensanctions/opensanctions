import re
from pprint import pprint  # noqa
from datetime import datetime

from opensanctions.util import EntityEmitter, normalize_country

SPLITS = r'(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)'  # noqa


def clean_date(date):
    try:
        dt = datetime.strptime(date, '%d-%b-%Y')
        return dt.date().isoformat()
    except Exception:
        pass


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
        if '*' in name:
            name, _ = name.rsplit('*', 1)
        # name = re.sub(r'\* *\d{1,4}$', '', name)
        name = name.strip(')').strip('(').strip(',')
        name = name.strip()
        clean_names.append(name)
    return clean_names


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for table in res.html.findall('.//table'):
            if 'List of Debarred' not in table.get('summary', ''):
                continue
            rows = table.findall('.//tr')
            for row in rows:
                tds = row.findall('./td')
                if len(tds) != 6:
                    continue
                values = [clean_value(td) for td in tds]
                entity = emitter.make('LegalEntity')
                entity.make_id(*values)

                names = clean_name(values[0])
                if not len(names):
                    context.log.warning("No name: %r", values)
                    continue

                entity.add('name', names[0])
                entity.add('address', values[1])
                entity.add('country', normalize_country(values[2]))
                for name in names[1:]:
                    entity.add('alias', name)

                sanction = emitter.make('Sanction')
                sanction.make_id('Sanction', entity.id)
                sanction.add('authority', 'World Bank Debarrment')
                sanction.add('program', values[5])
                sanction.add('startDate', clean_date(values[3]))
                sanction.add('endDate', clean_date(values[4]))
                sanction.add('sourceUrl', data.get('url'))
                emitter.emit(entity)
                emitter.emit(sanction)
