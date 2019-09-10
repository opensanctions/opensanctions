from urllib.parse import urljoin
from normality import slugify, collapse_spaces, stringify
from pprint import pprint  # noqa
from datetime import datetime

from opensanctions import constants
from opensanctions.util import EntityEmitter
from opensanctions.util import jointext

SEXES = {
    'M': constants.MALE,
    'F': constants.FEMALE,
}

NOTICE_URL = 'https://ws-public.interpol.int/notices/v1/red?&nationality=%s&arrestWarrantCountryId=%s'

def parse_date(date):
    date = datetime.strptime(date, '%Y/%m/%d')
    return date.date()

def get_value(el):
    if el is None:
        return
    text = stringify(el.get('value'))
    if text is not None:
        return collapse_spaces(text)


def index(context, data):
    with context.http.rehash(data) as result:
        doc = result.html
        nationalities = doc.findall(".//select[@id='nationality']//option")
        nationalities = [get_value(el) for el in nationalities]
        nationalities = [x for x in nationalities if x is not None]
        wanted_by = doc.findall(".//select[@id='arrestWarrantCountryId']//option")  # noqa
        wanted_by = [get_value(el) for el in wanted_by]
        wanted_by = [x for x in wanted_by if x is not None]
        combinations = [(n, w) for n in nationalities for w in wanted_by]
        for (nationality, wanted_by) in combinations:
            url = NOTICE_URL % (nationality, wanted_by)
            context.emit(data={'url': url})


def parse_noticelist(context, data):
    with context.http.rehash(data) as res:
        res = res.json
        notices = res['_embedded']['notices']
        for notice in notices:
            url = notice['_links']['self']['href']
            context.emit(data={'url': url})


def parse_notice(context, data):
    with context.http.rehash(data) as res:
        res = res.json
        first_name = res['forename'] or ''
        last_name = res['name'] or ''
        dob = res['date_of_birth']
        nationalities = res['nationalities']
        place_of_birth = res['place_of_birth']
        warrants = [
            (warrant['charge'], warrant['issuing_country_id'])
            for warrant in res['arrest_warrants']  # noqa
        ]
        gender = SEXES.get(res['sex_id'])
        emitter = EntityEmitter(context)
        entity = emitter.make('Person')
        entity.make_id(first_name, last_name, res['entity_id'])
        entity.add('name', first_name + ' ' + last_name)
        entity.add('firstName', first_name)
        entity.add('lastName', last_name)
        entity.add('nationality', nationalities)
        for charge, country in warrants:
            entity.add('program', country)
            entity.add('summary', charge)
        entity.add('gender', gender)
        entity.add('birthPlace', place_of_birth)
        entity.add('birthDate', parse_date(dob))
        entity.add('sourceUrl', res['_links']['self']['href'])
        entity.add('keywords', 'REDNOTICE')
        entity.add('keywords', 'CRIME')
        emitter.emit(entity)
        emitter.finalize()
