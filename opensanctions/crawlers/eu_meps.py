from pprint import pprint  # noqa

from opensanctions.models import Entity


def split_name(name):
    for i in range(len(name)):
        last_name = name[i:].strip()
        if last_name == last_name.upper():
            last_name = last_name.strip()
            first_name = name[:i].strip()
            return first_name, last_name


def parse_entry(context, node):
    entity = Entity.create('eu-meps', node.findtext('.//id'))
    entity.type = Entity.TYPE_INDIVIDUAL
    entity.name = node.findtext('.//fullName')
    entity.first_name, entity.last_name = split_name(entity.name)

    group = node.findtext('.//nationalPoliticalGroup') or ''
    entity.summary = '%s (%s)' % (node.findtext('.//politicalGroup') or '',
                                  group)

    nationality = entity.create_nationality()
    nationality.country = node.findtext('.//country')
    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    for node in res.xml.findall('.//mep'):
        parse_entry(context, node)
