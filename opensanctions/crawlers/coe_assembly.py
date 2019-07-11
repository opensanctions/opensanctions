import string
from pprint import pprint  # noqa
from urllib.parse import urljoin

from opensanctions.util import EntityEmitter
from opensanctions.util import normalize_country

URL = 'http://www.assembly.coe.int/nw/xml/AssemblyList/MP-Alpha-EN.asp?initial=%s&offset=0'  # noqa


def parse_entry(emitter, entry):
    link = entry.find('.//a')
    url = urljoin(URL, link.get('href'))
    _, member_id = url.rsplit('=', 1)
    person = emitter.make('Person')
    person.make_id(member_id)
    person.add('name', link.text)
    person.add('sourceUrl', url)
    last_name, first_name = link.text.split(', ', 1)
    person.add('lastName', last_name)
    person.add('firstName', first_name)
    person.add('position', entry.findtext('.//span[@class="fonction"]'))
    role, country = entry.findall('.//span[@class="infos"]')
    person.add('summary', role.text_content().strip())
    country = normalize_country(country.text_content().strip())
    person.add('nationality', country)
    person.add('keywords', ['PEP', 'PACE'])
    emitter.emit(person)


def parse(context, data):
    emitter = EntityEmitter(context)
    seen = set()
    for letter in string.ascii_uppercase:
        url = URL % letter
        while True:
            context.log.info("URL: %s", url)
            res = context.http.get(url)
            doc = res.html
            for member in doc.findall('.//ul[@class="member-results"]/li'):
                parse_entry(emitter, member)

            seen.add(url)
            url = None
            for a in doc.findall('.//div[@id="pagination"]//a'):
                next_url = urljoin(URL, a.get('href'))
                if next_url not in seen:
                    url = next_url
            if url is None:
                break
    emitter.finalize()
