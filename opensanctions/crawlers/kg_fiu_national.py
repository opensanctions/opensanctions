from pprint import pprint  # noqa
from datetime import datetime
import re

from opensanctions.util import EntityEmitter
from opensanctions.util import jointext


ORG_HEADER = ["No", "Last Name", "Name", "Middle Name", "Date of birth",
              "Place of birth", "Reason for inclusion",
              "Category of entity", "Date of inclusion"]

PER_HEADER = ["No", "Name", "Reason for inclusion",
              "Category of entity", "Date of inclusion"]


def has_integer_id(row):
    try:
        int(row[0])
        return True
    except Exception:
        return False


def parse_date(date):
    pattern = re.compile(r'[^0-9\.]')
    if date is not None:
        date = pattern.sub('', date)
        try:
            date = datetime.strptime(date, '%d.%m.%Y')
        except ValueError:
            date = datetime.strptime(date, '%Y')
        return date.date()


def extract_rows(node):
    for table in node.findall('.//table'):
        for row in table.findall('./row'):
            cells = []
            for cell in row.findall('./cell'):
                cells.append(cell.text)
            if not len(cells):
                continue
            if not has_integer_id(cells[0]):
                continue
            if len(cells) == 5:
                yield {k: v for k, v in zip(PER_HEADER, cells)}
            if len(cells) == 9:
                yield {k: v for k, v in zip(ORG_HEADER, cells)}


def parse_individual(emitter, data):
    person = emitter.make('Person')
    name = jointext(data["Name"], data["Middle Name"], data["Last Name"])
    person.make_id(name, data["Reason for inclusion"])
    person.add('name', name)
    person.add('lastName', data["Last Name"])
    person.add('firstName', data["Name"])
    person.add('fatherName', data["Middle Name"])
    # Some records have multiple dobs
    dob = data["Date of birth"]
    if dob is not None:
        dobs = dob.split()
        for date in dobs:
            person.add('birthDate', parse_date(date))
    person.add('birthPlace', data["Place of birth"])

    sanction = emitter.make('Sanction')
    sanction.make_id('Sanction', person.id)
    sanction.add('entity', person)
    sanction.add('authority', 'Kyrgyz Financial Intelligence Unit')
    sanction.add('reason', data["Reason for inclusion"])
    sanction.add('program', data["Category of entity"])
    sanction.add('startDate', parse_date(data["Date of inclusion"]))
    emitter.emit(person)
    emitter.emit(sanction)


def parse_organisation(emitter, data):
    entity = emitter.make('LegalEntity')
    entity.make_id(data["Name"], data["Reason for inclusion"])
    sanction = emitter.make('Sanction')
    sanction.make_id('Sanction', entity.id)
    sanction.add('entity', entity)
    sanction.add('authority', 'Kyrgyz Financial Intelligence Unit')
    sanction.add('reason', data["Reason for inclusion"])
    sanction.add('program', data["Category of entity"])
    sanction.add('startDate', parse_date(data["Date of inclusion"]))
    names = data["Name"].split(',')
    entity.add('name', names[0])
    entity.add('alias', names[1:])
    emitter.emit(entity)
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for row in extract_rows(res.xml):
            if len(row) == 5:
                parse_organisation(emitter, row)
            if len(row) == 9:
                parse_individual(emitter, row)
    emitter.finalize()
