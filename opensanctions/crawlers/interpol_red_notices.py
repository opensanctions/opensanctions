from urlparse import urljoin
from normality import slugify, collapse_spaces, stringify
from itertools import count
from pprint import pprint  # noqa

from memorious.helpers import make_id

from opensanctions.models import Entity

SEXES = {
    'Male': Entity.GENDER_MALE,
    'Female': Entity.GENDER_FEMALE,
}


def element_text(el):
    if el is None:
        return
    text = stringify(unicode(el.text_content()))
    if text is not None:
        return collapse_spaces(text)


def scrape_case(context, data):
    url = data.get('url')
    res = context.http.get(url)
    doc = res.html
    name = element_text(doc.find('.//div[@class="nom_fugitif_wanted"]'))
    if name is None or name == 'Identity unknown':
        return
    uid = make_id(url)
    entity = Entity.create('interpol-red-notices', uid)
    entity.url = url
    entity.type = entity.TYPE_INDIVIDUAL
    entity.name = name
    entity.program = element_text(doc.find('.//span[@class="nom_fugitif_wanted_small"]'))  # noqa

    if ', ' in name:
        last, first = name.split(', ', 1)
        alias = entity.create_alias()
        alias.name = ' '.join((first, last))

    for row in doc.findall('.//div[@class="bloc_detail"]//tr'):
        title, value = row.findall('./td')
        name = slugify(element_text(title), sep='_')
        value = element_text(value)
        if value is None:
            continue
        if name == 'charges':
            entity.summary = value
        elif name == 'present_family_name':
            entity.last_name = value
        elif name == 'forename':
            entity.first_name = value
        elif name == 'nationality':
            for country in value.split(', '):
                nationality = entity.create_nationality()
                nationality.country = country
        elif name == 'sex':
            entity.gender = SEXES[value]
        elif name == 'date_of_birth':
            birth_date = entity.create_birth_date()
            birth_date.date = value.split('(')[0]
        elif name == 'place_of_birth':
            birth_place = entity.create_birth_place()
            birth_place.date = value

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def scrape(context, data):
    url = context.params.get('url')
    seen = set()
    for i in count(0):
        p = i * 9
        res = context.http.get(url % p)
        doc = res.html
        links = doc.findall('.//div[@class="wanted"]//a')
        if not len(links):
            break
        for link in links:
            case_url = urljoin(url, link.get('href'))
            if case_url in seen:
                continue
            seen.add(case_url)
            context.emit(data={'url': case_url})
