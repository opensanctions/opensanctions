import string
from pprint import pprint  # noqa
from urlparse import urljoin

from opensanctions.models import Entity


def parse_entry(context, entry):
    link = entry.find('.//a')
    url_template = context.params.get('url')
    url = urljoin(url_template, link.get('href'))
    _, member_id = url.rsplit('=', 1)
    entity = Entity.create('coe_assembly', member_id)
    entity.type = Entity.TYPE_INDIVIDUAL
    entity.url = url
    entity.last_name, entity.first_name = link.text.split(', ', 1)
    entity.function = entry.findtext('.//span[@class="fonction"]')
    role, country = entry.findall('.//span[@class="infos"]')
    entity.summary = role.text_content().strip()
    nationality = entity.create_nationality()
    nationality.country = country.text_content().strip()

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    for letter in string.uppercase:
        seen = set()
        url_template = context.params.get('url')
        url = url_template % letter
        while True:
            res = context.http.get(url)
            doc = res.html
            for member in doc.findall('.//ul[@class="member-results"]/li'):
                parse_entry(context, member)

            seen.add(url)
            url = None
            for a in doc.findall('.//div[@id="pagination"]//a'):
                next_url = urljoin(url_template, a.get('href'))
                if next_url not in seen:
                    url = next_url
            if url is None:
                break
