from urllib.parse import urljoin
from normality import slugify, collapse_spaces, stringify
from pprint import pprint  # noqa

from opensanctions.util import EntityEmitter, Constants

SEXES = {
    'Male': Constants.GENDER_MALE,
    'Female': Constants.GENDER_FEMALE,
}


def element_text(el):
    if el is None:
        return
    text = stringify(el.text_content())
    if text is not None:
        return collapse_spaces(text)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as result:
        doc = result.html
        name = element_text(doc.find('.//div[@class="nom_fugitif_wanted"]'))
        if name is None or name == 'Identity unknown':
            return
        entity = emitter.make('Person')
        entity.make_id(data.get('url'))
        entity.add('name', name)
        entity.add('sourceUrl', data.get('url'))
        wanted = element_text(doc.find('.//span[@class="nom_fugitif_wanted_small"]'))  # noqa
        entity.add('program', wanted)

        if ', ' in name:
            last, first = name.split(', ', 1)
            entity.add('alias', ' '.join((first, last)))

        for row in doc.findall('.//div[@class="bloc_detail"]//tr'):
            title, value = row.findall('./td')
            name = slugify(element_text(title), sep='_')
            value = element_text(value)
            if value is None:
                continue
            if name == 'charges':
                entity.add('summary', value)
            elif name == 'present_family_name':
                entity.add('lastName', value)
            elif name == 'forename':
                entity.add('firstName', value)
            elif name == 'nationality':
                for country in value.split(', '):
                    entity.add('nationality', country)
            elif name == 'sex':
                entity.add('gender', SEXES[value])
            elif name == 'date_of_birth':
                entity.add('birthDate', value.split('(')[0])
            elif name == 'place_of_birth':
                entity.add('birthPlace', value)
        emitter.emit(entity)


def index(context, data):
    page = data.get('page', 0)
    url = context.params.get('url')
    url = url % (page * 9)
    res = context.http.get(url)
    doc = res.html
    links = doc.findall('.//div[@class="wanted"]//a')
    if not len(links):
        return
    for link in links:
        case_url = urljoin(url, link.get('href'))
        context.emit(data={'url': case_url})
    context.recurse(data={'page': page + 1})
