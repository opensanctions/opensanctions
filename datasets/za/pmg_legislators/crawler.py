from followthemoney.helpers import post_summary
from rigour.ids.wikidata import is_qid
from typing import Dict, Optional
from urllib.parse import unquote
import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise

# add headers to the request

PHONE_SPLITS = [",", "/", "(1)", "(2)", "(3)", "(4)", "(5)", "(6)", "(7)", "(8)"]
PHONE_REMOVE = re.compile(r"(ex|ext|extension|fax|tel|\:|\-)", re.IGNORECASE)

# List of regex patterns for positions of interest
POSITIONS_OF_INTEREST = [
    re.compile(r"National\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the National\s+Assembly", re.IGNORECASE),
    re.compile(r"National\s+Council of Provinces", re.IGNORECASE),
    re.compile(r"Minister of", re.IGNORECASE),
    re.compile(r"Provincial\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the Provincial Legislature", re.IGNORECASE),
    re.compile(r"Member of the Executive Committee", re.IGNORECASE),
    re.compile(r"National\s+Executive", re.IGNORECASE),
]

GOV_POSITIONS = [
    re.compile(r"National\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the National\s+Assembly", re.IGNORECASE),
    re.compile(r"Minister of", re.IGNORECASE),
    re.compile(r"National\s+Executive", re.IGNORECASE),
]

LEG_POSITIONS = [
    re.compile(r"National\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the National\s+Assembly", re.IGNORECASE),
    re.compile(r"Provincial\s+Legislature", re.IGNORECASE),
    re.compile(r"Member of the Provincial Legislature", re.IGNORECASE),
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


def crawl_person(context: Context, person_data: dict, organizations, events):
    person_id = person_data.get("id")

    person_qid = None
    person_slug = None
    for ident in person_data.pop("identifiers"):
        identifier = ident.get("identifier")
        scheme = ident.get("scheme")

        if scheme == "wikidata" and is_qid(identifier):
            person_qid = identifier
            break

        if scheme == "pombola-slug":
            person_id = identifier
            person_slug = identifier

    person = context.make("Person")
    if person_qid:
        person.id = person_qid
    else:
        person.id = person_entity_id(context, person_id)
    person.add("name", person_data.get("name"))
    person.add("alias", [o.get("name") for o in person_data.get("other_names", [])])
    person.add("gender", person_data.get("gender"))
    person.add("firstName", person_data.get("given_name"))
    person.add("lastName", person_data.get("family_name"))
    person.add("birthDate", person_data.get("birth_date"))
    person.add("deathDate", person_data.get("death_date"))
    person.add("notes", person_data.pop("summary", None))
    person.add("title", person_data.pop("honorific_prefix", None))
    person.add("wikidataId", person_qid)

    if person_slug:
        source_url = f"https://pa.org.za/person/{person_slug}/"
        person.add("sourceUrl", source_url)

    for link in person_data.pop("links", []):
        url = link.get("url")
        if link.get("note") in ("website", "blog", "twitter", "facebook"):
            person.add("website", url)

    for contact_detail in person_data.pop("contact_details", []):
        value = contact_detail.get("value")
        if "email" == contact_detail.get("type"):
            person.add("email", clean_emails(value))
        if "address" == contact_detail.get("type"):
            person.add("address", value)
        if "postal_address" == contact_detail.get("type"):
            person.add("address", value)

    for membership in person_data.pop("memberships", []):
        crawl_membership(
            context,
            person,
            membership,
            organizations,
            events,
            person_data.get("birth_date"),
            person_data.get("death_date"),
        )


def crawl_membership(
    context: Context,
    entity: Entity,
    membership: dict,
    organizations: Dict[str, str],
    events,
    birth_date: str,
    death_date: str,
) -> Optional[str]:
    org_id = membership.get("organization_id")
    org_name = organizations.get(org_id)
    if not org_name:
        # context.log.error(f"Organization with ID {org_id} not found.")
        return None

    role = membership.get("role")
    if role is None:
        # context.log.error("Role is missing in membership data.")
        return None

    # Creating the position property
    position_property = post_summary(
        org_name, role, [membership.get("start_date")], [membership.get("end_date")], []
    )
    entity.add("position", position_property)

    # context.log.info(f"Processed membership for person ID: {person_id}")
    position_label = f"{role.title()} of the {org_name}"

    # Check if the role matches the positions of interest
    if not any(re.findall(regex, position_label) for regex in POSITIONS_OF_INTEREST):
        # context.log.info(f"Skipping role {role} not of interest.")
        return None

    position = h.make_position(
        context,
        position_label,
        country="za",
        topics=None,
    )

    # Always PEP because filtered by known position label patterns
    categorisation = categorise(context, position, is_pep=True)

    period_id = membership.get("legislative_period_id")
    period = events.get(period_id, {})
    role = membership.pop("role", None)
    role = role or period.get("name")

    # If the role is not "member", we might want to log a warning.
    # if role != "member":
    #     context.log.warning("Unexpected role", role=role)

    starts = [membership.get("start_date"), period.get("start_date")]
    ends = [membership.get("end_date"), period.get("end_date")]
    position_property = post_summary(org_name, role, starts, ends, [])

    # Creating and emitting the Occupancy entity
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        no_end_implies_current=False,
        start_date=membership.pop("start_date", ""),  # or period.pop("start_date", ""),
        end_date=membership.pop("end_date", ""),  # or period.pop("end_date", ""),
        categorisation=categorisation,
        birth_date=birth_date,
        death_date=death_date,
    )

    if occupancy:
        context.emit(entity, target=True)
        context.emit(position)
        context.emit(occupancy)

    return None


def person_entity_id(context: Context, person_id: str) -> str:
    return context.make_slug(person_id)


def crawl(context: Context):
    # Fetch data from the provided URL
    data = context.fetch_json(context.data_url)

    # Extract and prepare organizations
    organizations = {org["id"]: org["name"] for org in data.get("organizations")}

    # Collect all memberships from all persons
    persons = data.get("persons")

    # Prepare events
    events = {e.get("id"): e for e in data.pop("events")}

    # Prepare birth and death dates dictionaries
    birth_dates: Dict[str, str] = {}
    death_dates: Dict[str, str] = {}

    for person in persons:
        death_date = person.get("death_date", None)
        if death_date is not None:
            death_dates[person.get("id")] = death_date

        birth_date = person.get("birth_date")
        if birth_date is not None:
            birth_dates[person.get("id")] = birth_date

    # Process persons
    for person in persons:
        crawl_person(context, person, organizations, events)
