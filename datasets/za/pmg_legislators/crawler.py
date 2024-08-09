from typing import Dict, Optional, Set
from followthemoney.helpers import post_summary
from zavod import Context
from zavod import helpers as h
import re
from urllib.parse import unquote
# from zavod.logic.pep import categorise

# add headers to the request

PHONE_SPLITS = [",", "/", "(1)", "(2)", "(3)", "(4)", "(5)", "(6)", "(7)", "(8)"]
PHONE_REMOVE = re.compile(r"(ex|ext|extension|fax|tel|\:|\-)", re.IGNORECASE)

# List of regex patterns for positions of interest
POSITIONS_OF_INTEREST = [
    re.compile(r"National\s+Legislature", re.IGNORECASE),
    re.compile(r"National\s+Assembly", re.IGNORECASE),
    re.compile(r"National\s+Council of Provinces", re.IGNORECASE),
    re.compile(r"Minister of", re.IGNORECASE),
    re.compile(r"Provincial\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the Provincial Legislature", re.IGNORECASE),
    re.compile(r"Member of the Executive Committee", re.IGNORECASE),
    re.compile(r"National\s+Executive", re.IGNORECASE),
]


def clean_emails(emails):
    out = []
    for email in h.multi_split(emails, ["/", ",", " or "]):
        if email is None:
            return
        email = unquote(email)
        email = email.strip()
        email = email.rstrip(".")
        out.append(email)
    return out


def clean_phones(phones):
    out = []
    for phone in h.multi_split(phones, PHONE_SPLITS):
        phone = PHONE_REMOVE.sub("", phone)
        out.append(phone)
    return out


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
    person.add("notes", data.pop("summary", None))
    person.add("title", data.pop("honorific_prefix", None))
    person.add("topics", "role.pep")

    for link in data.pop("links", []):
        url = link.get("url")
        if link.get("note") in ("website", "blog", "twitter", "facebook"):
            person.add("website", url)

    for contact_detail in data.pop("contact_details", []):
        value = contact_detail.get("value")
        if "email" == contact_detail.get("type"):  # might want to remove later
            person.add("email", clean_emails(value))
        if "phone" == contact_detail.get("type"):
            person.add("phone", clean_phones(value))
        if "cell" == contact_detail.get("type"):
            person.add("phone", clean_phones(value))
        if "address" == contact_detail.get("type"):
            person.add("address", value)
        if "postal_address" == contact_detail.get("type"):
            person.add("address", value)

    for ident in data.pop("identifiers", []):
        identifier = ident.get("identifier")
        scheme = ident.get("scheme")
        if scheme == "wikidata" and identifier.startswith("Q"):
            person.add("wikidataId", identifier)

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
    org_id = data.get("organization_id")
    role = data.get("role")
    role = data.get("role")
    if role is None:
        # context.log.error("Role is missing in membership data.")
        return None

    # Check if the role matches the positions of interest
    if not any(re.findall(position, role) for position in POSITIONS_OF_INTEREST):
        context.log.info(f"Skipping role {role} not of interest.")
        return None

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
