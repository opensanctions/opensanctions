from pprint import pprint  # noqa

from opensanctions.models import Entity
from followthemoney import model

def split_name(name):
    for i in range(len(name)):
        last_name = name[i:].strip()
        if last_name == last_name.upper():
            last_name = last_name.strip()
            first_name = name[:i].strip()
            return first_name, last_name


def parse_entry(context, node):
    person = model.make_entity("Person")
    person.make_id(node.findtext('.//id'))
    name = node.findtext('.//fullName')
    first_name, last_name = split_name(name)
    person.add("name", name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    group = node.findtext('.//nationalPoliticalGroup') or ''
    summary = '%s (%s)' % (node.findtext('.//politicalGroup') or '', group)
    person.add("summary", summary)
    country = node.findtext('.//country')
    person.add("nationality", country)
    pprint(person.to_dict())
    context.emit(data=person.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    for node in res.xml.findall('.//mep'):
        parse_entry(context, node)
