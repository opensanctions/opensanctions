import re
from datetime import datetime
from pprint import pprint  # noqa

from opensanctions.util import EntityEmitter
from opensanctions.util import jointext, normalize_country


def parse_date(date):
    if date is None:
        return
    date = date.replace('.', '').strip()
    if ';' in date:
        date, _ = date.split(';', 1)
    try:
        return datetime.strptime(date, '%d %b %Y').date().isoformat()
    except Exception:
        match = re.search(r'\d{4}', date)
        if match:
            return match.group(0)


def parse_entry(emitter, entry):
    entity = emitter.make('LegalEntity')
    if entry.findtext('./type-entry') == '2':
        entity = emitter.make('Person')
    entity.make_id(entry.findtext('number-entry'))

    sanction = emitter.make('Sanction')
    sanction.make_id('Sanction', entity.id)
    sanction.add('entity', entity)
    sanction.add('authority', 'State Financial Monitoring Service of Ukraine')
    sanction.add('sourceUrl', 'http://www.sdfm.gov.ua/articles.php?cat_id=87&lang=en')  # noqa
    sanction.add('program', entry.findtext('./program-entry'))
    date_entry = entry.findtext('./date-entry')
    if date_entry:
        date = datetime.strptime(date_entry, '%Y%m%d').date()
        sanction.add('startDate', date)

    for aka in entry.findall('./aka-list'):
        first_name = aka.findtext('./aka-name1')
        entity.add('firstName', first_name, quiet=True)
        second_name = aka.findtext('./aka-name2')
        entity.add('secondName', second_name, quiet=True)
        third_name = aka.findtext('./aka-name3')
        entity.add('middleName', third_name, quiet=True)
        last_name = aka.findtext('./aka-name4')
        entity.add('lastName', last_name, quiet=True)
        name = jointext(first_name, second_name, third_name, last_name)
        if aka.findtext('type-aka') == 'N':
            entity.add('name', name)
        else:
            if aka.findtext('./quality-aka') == '2':
                entity.add('weakAlias', name)
            else:
                entity.add('alias', name)

    for node in entry.findall('./title-list'):
        entity.add('title', node.text, quiet=True)

    for doc in entry.findall('./document-list'):
        reg = doc.findtext('./document-reg')
        number = doc.findtext('./document-id')
        country = normalize_country(doc.findtext('./document-country'))
        passport = emitter.make('Passport')
        passport.make_id('Passport', entity.id, reg, number, country)
        passport.add('holder', entity)
        passport.add('passportNumber', number)
        passport.add('summary', reg)
        passport.add('country', country)
        emitter.emit(passport)

    for doc in entry.findall('./id-number-list'):
        entity.add('idNumber', doc.text)

    for node in entry.findall('./address-list'):
        entity.add('address', node.findtext('./address'))

    for pob in entry.findall('./place-of-birth-list'):
        entity.add('birthPlace', pob.text, quiet=True)

    for dob in entry.findall('./date-of-birth-list'):
        entity.add('birthDate', parse_date(dob.text), quiet=True)

    for nat in entry.findall('./nationality-list'):
        entity.add('nationality', normalize_country(nat.text), quiet=True)

    emitter.emit(entity)
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for entry in res.xml.findall('.//acount-list'):
            parse_entry(emitter, entry)
    emitter.finalize()
