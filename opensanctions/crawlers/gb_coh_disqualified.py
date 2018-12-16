import os
from pprint import pprint  # noqa
from urllib.parse import urljoin

from opensanctions.util import EntityEmitter, normalize_country


API_KEY = os.environ.get("MEMORIOUS_COH_API_KEY")
AUTH = (API_KEY, '')
SEARCH_URL = 'https://api.companieshouse.gov.uk/search/disqualified-officers'
API_URL = 'https://api.companieshouse.gov.uk/disqualified-officers/natural/%s'
WEB_URL = 'https://beta.companieshouse.gov.uk/register-of-disqualifications/A'


def officer(context, data):
    emitter = EntityEmitter(context)
    officer_id = data.get('officer_id')
    url = API_URL % officer_id
    with context.http.get(url, auth=AUTH) as res:
        if res.status_code != 200:
            return
        data = res.json
        person = emitter.make('Person')
        person.make_id(officer_id)
        source_url = urljoin(WEB_URL, data.get('links', {}).get('self', '/'))
        person.add('sourceUrl', source_url)

        last_name = data.pop('surname', None)
        person.add('lastName', last_name)
        forename = data.pop('forename', None)
        person.add('firstName', forename)
        other_forenames = data.pop('other_forenames', None)
        person.add('middleName', other_forenames)

        name = (forename, other_forenames, last_name)
        name = [n for n in name if n is not None and len(n)]
        name = ' '.join(name)
        person.add('name', name)
        person.add('title', data.pop('title', None))

        nationality = normalize_country(data.pop('nationality', None))
        person.add('nationality', nationality)
        person.add('birthDate', data.pop('date_of_birth', None))

        for disqual in data.pop('disqualifications', []):
            case = disqual.get('case_identifier')
            sanction = emitter.make('Sanction')
            sanction.make_id(person.id, case)
            sanction.add('entity', person)
            sanction.add('authority', 'UK Companies House')
            sanction.add('program', case)
            sanction.add('startDate', disqual.pop('disqualified_from', None))
            sanction.add('endDate', disqual.pop('disqualified_until', None))
            emitter.emit(sanction)

            address = disqual.pop('address', {})
            locality = address.get('locality')
            postal_code = address.get('postal_code')
            if locality and postal_code:
                locality = '%s %s' % (locality, postal_code)
            street = address.get('address_line_1')
            premises = address.get('premises')
            if street and premises:
                street = '%s %s' % (street, premises)
            parts = (street, address.get('address_line_2'),
                     locality, address.get('region'))
            parts = [p for p in parts if p is not None]
            parts = ', '.join(parts)
            person.add('address', parts)
        emitter.emit(person)


def pages(context, data):
    with context.http.rehash(data) as res:
        doc = res.html
        for direct in doc.findall('.//table//a'):
            ref = direct.get('href')
            _, officer_id = ref.rsplit('/', 1)
            context.emit(data={'officer_id': officer_id})

        for a in doc.findall('.//ul[@id="pager"]/li/a'):
            next_title = a.text.strip()
            if next_title == 'Next':
                url = urljoin(data.get('url'), a.get('href'))
                context.emit(rule='url', data={'url': url})


def alphabetical(context, data):
    with context.http.rehash(data) as res:
        doc = res.html
        for a in doc.findall('.//ul[@id="alphabetical-pager"]/li/a'):
            url = urljoin(WEB_URL, a.get('href'))
            context.emit(data={'url': url})
