# from urllib.parse import urljoin, unquote
from typing import Dict, Optional, Set
from followthemoney.helpers import post_summary
from zavod import Context
# from zavod import helpers as h
# from zavod.logic.pep import categorise

# add headers to the request


def parse_person(context: Context, data: dict):
    person_id = data.get("id")
    if not person_id:
        context.log.error(f"Missing ID in person data: {data}")
        return
    person = context.make("Person")
    person.id = person_entity_id(context, person_id)
    person.add("name", data.get("name"))
    person.add("alias", [o.get("name") for o in data.get("other_names", [])])
    person.add("gender", data.get("gender"))
    person.add("firstName", data.get("given_name"))
    person.add("lastName", data.get("family_name"))
    person.add("birthDate", data.get("birth_date"))
    person.add("deathDate", data.get("death_date"))
    context.emit(person, target=True)
    return person


def parse_membership(
    context: Context, data: dict, organizations: Dict[str, str]
) -> Optional[str]:
    person_id = data.get("person_id")
    if not person_id:
        context.log.error("Missing person_id in membership data.")
        return None
    org_id = data.get("organization_id")
    org_name = organizations.get(org_id)
    if not org_name:
        context.log.error(f"Organization with ID {org_id} not found.")
        return None
    role = data.get("role")
    position_property = post_summary(
        org_name, role, [data.get("start_date")], [data.get("end_date")], []
    )
    person = context.make("Person")
    person.id = person_entity_id(context, person_id)
    person.add("position", position_property)
    context.emit(person, target=True)
    # context.log.info(f"Processed membership for person ID: {person_id}")
    return person_id


def person_entity_id(context: Context, person_id: str) -> str:
    return context.make_slug(person_id)


def crawl(context: Context):
    data = context.fetch_json(context.data_url)
    if not data:
        context.log.error("No data found.")
        return

    persons = data.get("persons", [])
    organizations = {org["id"]: org["name"] for org in data.get("organizations", [])}
    all_memberships = []

    # Collect all memberships from all persons
    for person in persons:
        memberships = person.get("memberships", [])
        for membership in memberships:
            membership["person_id"] = person.get("id")
            all_memberships.append(membership)

    if not all_memberships:
        context.log.error("No 'memberships' key found or it's empty.")
        return

    peps: Set[str] = set()

    # Process memberships
    for membership in all_memberships:
        person_id = parse_membership(context, membership, organizations)
        if person_id:
            peps.add(person_id)
            # context.log.info(f"Processed person ID: {person_id}")

    # Process persons
    for person in persons:
        if person.get("id") in peps:
            parse_person(context, person)
