import re
from hashlib import sha1
from pprint import pprint  # noqa

from dateutil.parser import parse as dateutil_parse

from opensanctions.models import Entity


SPLITS = r'(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)'  # noqa


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
    url = context.params.get('url')
    res = context.http.rehash(data)
    doc = res.html
    for table in doc.findall('.//table'):
        if 'List of Debarred' not in table.get('summary', ''):
            continue
        rows = table.findall('.//tr')
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
                context.log.warning("No name: %r", values)
                continue

            entity = Entity.create('zz-wb-debarred', uid)
            entity.program = values[5]
            entity.name = names[0]
            entity.updated_at = dateutil_parse(values[3]).date().isoformat()
            entity.url = url
            for name in names[1:]:
                entity.create_alias(name=name)
            
            nationality = entity.create_nationality()
            nationality.country = values[2]

            address = entity.create_address()
            address.text = values[1]
            address.country = values[2]

            # pprint(entity.to_dict())
            context.emit(data=entity.to_dict())
