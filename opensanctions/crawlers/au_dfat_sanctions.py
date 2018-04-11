from pprint import pprint  # noqa

import pandas as pd

from opensanctions.models import Entity


def parse_entry(context, data):
    rows = data.get('rows')
    primary = rows[0]
    if primary['Type'] == 'Individual':
        type_ = Entity.TYPE_INDIVIDUAL
    else:
        type_ = Entity.TYPE_ENTITY

    entity = Entity.create('au-dfat-sanctions', primary.get('Reference'))
    entity.type = type_
    entity.url = 'http://dfat.gov.au/international-relations/security/sanctions/Pages/sanctions.aspx'  # noqa
    entity.name = primary['Name of Individual or Entity']
    entity.program = primary['Committees']
    entity.summary = primary['Additional Information']

    country = primary['Citizenship']
    if not isinstance(country, float):  # not NaN
        nationality = entity.create_nationality()
        nationality.country = country

    address = entity.create_address()
    address.text = primary['Address']

    birth_date_text = primary['Date of Birth']
    if not isinstance(birth_date_text, float):
        birth_date = entity.create_birth_date()
        birth_date.date = birth_date_text

    birth_place_text = primary['Place of Birth']
    if not isinstance(birth_place_text, float):
        birth_place = entity.create_birth_place()
        birth_place.place = birth_place_text

    if rows[1:]:
        for row in rows[1:]:
            alias = entity.create_alias()
            alias.name = row['Name of Individual or Entity']

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    xls = pd.ExcelFile(res.file_path)
    df = xls.parse(xls.sheet_names[0])
    batch = []
    for _, row in df.iterrows():
        row = row.to_dict()
        row['Control Date'] = str(row['Control Date'])
        if row['Name Type'] == 'Primary Name':
            batch = [row]
        elif row['Name Type'] == 'aka':
            batch.append(row)
        context.emit(data={
            'rows': batch
        })
