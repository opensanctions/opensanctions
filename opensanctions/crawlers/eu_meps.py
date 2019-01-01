from pprint import pprint  # noqa

from opensanctions.util import EntityEmitter, normalize_country


def split_name(name):
    for i in range(len(name)):
        last_name = name[i:].strip()
        if last_name == last_name.upper():
            last_name = last_name.strip()
            first_name = name[:i].strip()
            return first_name, last_name


def parse_node(emitter, node):
    person = emitter.make("Person")
    person.make_id(node.findtext('.//id'))
    name = node.findtext('.//fullName')
    person.add("name", name)
    first_name, last_name = split_name(name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    country = normalize_country(node.findtext('.//country'))
    person.add("nationality", country)
    emitter.emit(person)

    party_name = node.findtext('.//nationalPoliticalGroup')
    if party_name not in ['Independent']:
        party = emitter.make('Organization')
        party.make_id('nationalPoliticalGroup', party_name)
        party.add('name', party_name)
        party.add('country', country)
        emitter.emit(party)
        membership = emitter.make('Membership')
        membership.make_id(person.id, party.id)
        membership.add('member', person)
        membership.add('organization', party)
        emitter.emit(membership)

    group_name = node.findtext('.//politicalGroup')
    group = emitter.make('Organization')
    group.make_id('politicalGroup', group_name)
    group.add('name', group_name)
    group.add('country', 'eu')
    emitter.emit(group)
    membership = emitter.make('Membership')
    membership.make_id(person.id, group.id)
    membership.add('member', person)
    membership.add('organization', group)
    emitter.emit(membership)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for node in res.xml.findall('.//mep'):
            parse_node(emitter, node)
