from opensanctions.util import EntityEmitter


def index(context, data):
    with context.http.rehash(data) as res:
        for country in res.json:
            for legislature in country.get('legislatures', []):
                context.emit(data={
                    "country": country,
                    "legislature": legislature,
                    "url": legislature.get('popolo_url'),
                })


def parse(context, data):
    emitter = EntityEmitter(context)
    country = data.get('country', {}).get('code')
    with context.http.rehash(data) as res:
        persons = {}
        for person in res.json.get('persons', []):
            ep_id, ftm_id = parse_person(emitter, person, country)
            persons[ep_id] = ftm_id

        organizations = {}
        for organization in res.json.get('organizations', []):
            ep_id, ftm_id = parse_organization(emitter, organization, country)
            organizations[ep_id] = ftm_id

        for membership in res.json.get('memberships', []):
            parse_membership(emitter, membership, persons, organizations)


def parse_person(emitter, data, country):
    person_id = data.pop('id', None)
    person = emitter.make('Person')
    person.make_id(person_id)
    person.add('name', data.pop('name', None))
    person.add('alias', data.pop('sort_name', None))
    person.add('gender', data.pop('gender', None))
    person.add('title', data.pop('honorific_prefix', None))
    person.add('title', data.pop('honorific_suffix', None))
    person.add('firstName', data.pop('given_name', None))
    person.add('lastName', data.pop('family_name', None))
    person.add('fatherName', data.pop('patronymic_name', None))
    person.add('birthDate', data.pop('birth_date', None))
    person.add('deathDate', data.pop('death_date', None))
    person.add('email', data.pop('email', None))
    person.add('summary', data.pop('summary', None))
    person.add('keywords', ['PEP', 'PARL'])

    for other_name in data.pop('other_names', []):
        person.add('alias', other_name.get('name'))

    for identifier in data.pop('identifiers', []):
        if 'wikidata' == identifier.get('scheme'):
            person.add('wikidataId', identifier.get('identifier'))

    for link in data.pop('links', []):
        if 'Wikipedia' in link.get('note'):
            person.add('wikipediaUrl', link.get('url'))

    for contact_detail in data.pop('contact_details', []):
        if 'email' == contact_detail.get('type'):
            person.add('email', contact_detail.get('value'))
        if 'phone' == contact_detail.get('type'):
            person.add('phone', contact_detail.get('value'))

    # data.pop('image', None)
    emitter.emit(person)
    return person_id, person.id


def parse_organization(emitter, data, country):
    org_id = data.get('id')
    if data.get('name') == 'unknown':
        return org_id, None

    organization = emitter.make('Organization')
    organization.make_id(org_id)
    organization.add('name', data.get('name'))
    organization.add('summary', data.get('type'))
    organization.add('country', country)

    for identifier in data.get('identifiers', []):
        if 'wikidata' == identifier.get('scheme'):
            organization.add('wikidataId', identifier.get('identifier'))

    emitter.emit(organization)
    return org_id, organization.id


def parse_membership(emitter, data, persons, organizations):
    person_id = persons.get(data.get('person_id'))
    organization_id = organizations.get(data.get('organization_id'))
    on_behalf_of_id = organizations.get(data.get('on_behalf_of_id'))

    if person_id and organization_id:
        membership = emitter.make('Membership')
        membership.make_id(person_id, organization_id)
        membership.add('member', person_id)
        membership.add('organization', organization_id)
        membership.add('role', data.get('role'))

        for source in data.get('sources', []):
            membership.add('sourceUrl', source.get('url'))

        emitter.emit(membership)

    if person_id and on_behalf_of_id:
        membership = emitter.make('Membership')
        membership.make_id(person_id, on_behalf_of_id)
        membership.add('member', person_id)
        membership.add('organization', on_behalf_of_id)

        for source in data.get('sources', []):
            membership.add('sourceUrl', source.get('url'))

        emitter.emit(membership)
