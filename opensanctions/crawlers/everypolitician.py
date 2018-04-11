from datetime import datetime
from unicodecsv import DictReader

from pprint import pprint  # noqa
from opensanctions.models import Entity


GENDERS = {
    'male': Entity.GENDER_MALE,
    'female': Entity.GENDER_FEMALE,
    None: None,
    '': None
}


def parse_ts(ts):
    return datetime.fromtimestamp(int(ts)).date().isoformat()


def scrape_entity(context, data):
    row = data.get("row")
    legislature = data.get("legislature")
    country = data.get("country")
    if row.get('id') is None:
        context.log.warning("No ID for entry: %r", row)
    entity = Entity.create('everypolitician', row.get('id'))
    entity.type = entity.TYPE_INDIVIDUAL
    entity.updated_at = parse_ts(legislature.get('lastmod'))
    entity.name = row.get('name')
    entity.function = row.get('group')
    entity.program = legislature.get('name')
    entity.gender = GENDERS[row.get('gender')]

    nationality = entity.create_nationality()
    nationality.country = country.get('name')
    nationality.country_code = country.get('code')

    if row.get('name') != row.get('sort_name'):
        alias = entity.create_alias()
        alias.name = row.get('sort_name')

    # TODO: email
    # TODO: socialmedia
    # TODO: photograph
    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def scrape_csv(context, data):
    period = data.get("period")
    country = data.get("country")
    legislature = data.get("legislature")
    start_year = int(period.get('start_date')[:4])
    current_year = datetime.utcnow().year
    # Don't import the US 2nd continental congress (TM):
    if current_year - 10 > start_year:
        return
    res = context.http.get(period.get('csv_url'))
    with open(res.file_path, 'rb') as csvfile:
        for row in DictReader(csvfile):
            context.emit(data={
                "country": country,
                "legislature": legislature,
                "row": row
            })


def scrape(context, data):
    res = context.http.rehash(data)
    for country in res.json:
        for legislature in country.get('legislatures', []):
            for period in legislature.pop('legislative_periods', []):
                context.emit(data={
                    "country": country,
                    "legislature": legislature,
                    "period": period,
                })
