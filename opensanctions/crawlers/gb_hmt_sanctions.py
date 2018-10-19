from collections import defaultdict
from pprint import pprint  # noqa

from normality import stringify
from memorious.helpers import parse_date
import csv

from opensanctions.models import Entity, Identifier


ENTITY_TYPES = {
    'Individual': Entity.TYPE_INDIVIDUAL,
    'Entity': Entity.TYPE_ENTITY,
}


def fresh_value(seen, row, key):
    value = row.get(key)
    if value is None:
        return False
    if key not in seen or value in seen[key]:
        seen[key].add(value)
        return True
    return False


def parse_entry(context, data):
    group = data.get('group')
    rows = data.get('rows')
    seen = defaultdict(set)
    entity = Entity.create('gb-hmt-sanctions', group)
    for row in rows:
        entity.type = ENTITY_TYPES[row.pop('Group Type')]
        names = (row.pop('Name 1'), row.pop('Name 2'), row.pop('Name 3'),
                 row.pop('Name 4'), row.pop('Name 5'), row.pop('Name 6'))
        names = [n for n in names if len(n) > 0]
        row['_name'] = ' '.join(names)

        if fresh_value(seen, row, '_name'):
            name = entity
            if entity.name is not None:
                name = entity.create_alias()
                name.type = row.get('Alias Type')
            name.title = row.get('Title')
            name.last_name = names.pop()
            if len(names):
                name.first_name = names.pop(0)
            if len(names):
                name.second_name = names.pop(0)
            if len(names):
                name.third_name = ' '.join(names)

        if row.get('Regime'):
            entity.program = row.pop('Regime')
        if row.get('Position'):
            entity.function = row.pop('Position')
        if row.get('Other Information'):
            entity.summary = row.pop('Other Information')
        if row.get('Last Updated'):
            entity.updated_at = row.pop('Last Updated')

        if fresh_value(seen, row, 'DOB'):
            dob_text = row.get('DOB')
            if dob_text is None or not len(dob_text.strip()):
                continue
            dob = parse_date(dob_text)
            if dob is None and '/' in dob_text:
                _, dob = dob_text.rsplit('/', 1)
            birth_date = entity.create_birth_date()
            birth_date.date = stringify(dob)

        if fresh_value(seen, row, 'Town of Birth') or \
           fresh_value(seen, row, 'Country of Birth'):
            birth_place = entity.create_birth_place()
            birth_place.place = row.pop('Town of Birth')
            birth_place.country = row.pop('Country of Birth')

        addr = [row.pop('Address 1'), row.pop('Address 2'),
                row.pop('Address 3'), row.pop('Address 4'),
                row.pop('Address 5'), row.pop('Address 6')]
        addr_ids = addr + [row.get('Post/Zip Code'), row.get('Post/Zip Code')]
        row['_addr'] = ' '.join([a for a in addr_ids if len(a) > 0])
        if fresh_value(seen, row, '_addr'):
            address = entity.create_address()
            address.country = row.pop('Country')
            address.postal_code = row.pop('Post/Zip Code')
            address.text = ', '.join([a for a in addr if len(a) > 0])

        if fresh_value(seen, row, 'Passport Details'):
            identifier = entity.create_identifier()
            identifier.type = Identifier.TYPE_PASSPORT
            identifier.number = row.pop('Passport Details')
            identifier.country = row.get('Nationality')

        if fresh_value(seen, row, 'NI Number'):
            identifier = entity.create_identifier()
            identifier.type = Identifier.TYPE_NATIONALID
            identifier.number = row.pop('NI Number')
            identifier.country = row.get('Nationality')

        if fresh_value(seen, row, 'Nationality'):
            has_match = False
            text = row.pop('Nationality')
            for name in text.split(')'):
                code = name
                if code is not None:
                    nationality = entity.create_nationality()
                    nationality.country = name
                    has_match = True
            if not has_match:
                nationality = entity.create_nationality()
                nationality.country = text
    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    groups = {}
    res = context.http.rehash(data)

    with open(res.file_path, 'r', encoding='iso-8859-1') as csvfile:
        # ignore first line
        next(csvfile)
        for row in csv.DictReader(csvfile):
            group = int(float(row.pop('Group ID')))
            if group not in groups:
                groups[group] = []
            groups[group].append({k: stringify(v) if stringify(v) is not None else '' for k, v in row.items()})

    for group, rows in groups.items():
        context.emit(data={
            'group': group,
            'rows': rows
        })
