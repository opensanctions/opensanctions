import os
from pprint import pprint  # noqa
from urllib.parse import urljoin

from opensanctions.models import Entity


API_KEY = os.environ["MEMORIOUS_COH_API_KEY"]
AUTH = (API_KEY, '')
SEARCH_URL = 'https://api.companieshouse.gov.uk/search/disqualified-officers'
API_URL = 'https://api.companieshouse.gov.uk/disqualified-officers/%s/%s'
WEB_URL = 'https://beta.companieshouse.gov.uk/register-of-disqualifications/A'


def crawl_officer(context, data):
    officer_id = data.get('officer_id')
    for type_ in ('natural', 'corporate'):
        url = API_URL % (type_, officer_id)
        res = context.http.get(url, auth=AUTH)
        if res.status_code != 200:
            continue
        # TODO: check if this existed
        entity = Entity.create('gb-coh-disqualified', officer_id)
        data = res.json
        entity.title = data.get('title')
        entity.first_name = data.get('forename')
        entity.second_name = data.get('other_forenames')
        entity.last_name = data.get('surname')
        entity.summary = data.get('kind')
        entity.url = urljoin(WEB_URL, data.get('links', {}).get('self', '/'))

        if data.get('date_of_birth'):
            birth_date = entity.create_birth_date()
            birth_date.date = data.get('date_of_birth')

        if data.get('nationality'):
            nationality = entity.create_nationality()
            nationality.country = data.get('nationality')

        for disqualification in data.get('disqualifications', []):
            entity.program = disqualification.get('case_identifier')
            addr = disqualification.get('address')
            address = entity.create_address()
            address.street = addr.get('address_line_1')
            address.street_2 = addr.get('address_line_2')
            address.city = addr.get('locality')
            address.region = addr.get('region')
            address.postal_code = addr.get('postal_code')

        # pprint(entity.to_dict())
        context.emit(data=entity.to_dict())


def crawl_pages(context, data):
    url = data.get('url')
    base_url = url
    while url is not None:
        res = context.http.get(url)
        doc = res.html
        for direct in doc.findall('.//table//a'):
            ref = direct.get('href')
            _, officer_id = ref.rsplit('/', 1)
            context.emit(data={
                'officer_id': officer_id
            })

        url = None
        for a in doc.findall('.//ul[@id="pager"]/li/a'):
            next_title = a.text.strip()
            if next_title == 'Next':
                url = urljoin(base_url, a.get('href'))


def crawl_alphabetical(context, data):
    res = context.http.rehash(data)
    doc = res.html
    for a in doc.findall('.//ul[@id="alphabetical-pager"]/li/a'):
        url = urljoin(WEB_URL, a.get('href'))
        context.emit(data={'url': url})
