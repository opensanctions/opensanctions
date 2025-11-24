from typing import Any, cast
from zavod import Context, helpers as h

from zavod.stateful.positions import categorise

IGNORE = [
    "current_committee_memberships",
    "former_committee_memberships",
    "affiliations",
    "language_code",
    "type_code",
    "sort_name",
    "electoral_districts",
    "parliamentary_status",
    "as_member_of_parliament",
    "phone",
    "email",
    "nickname",
    "education",
    "parliamentary_term_suspended",
    "work_history",
    "government_memberships",
]
PEP_URL = "https://avoindata.eduskunta.fi/api/v1/memberofparliament/{person_id}/fi"


def translate_keys(context: Context, data: Any) -> Any:
    """Recursively translate keys in nested dictionaries and lists."""
    if isinstance(data, dict):
        translated = {}
        for key, value in data.items():
            # Translate the key
            new_key = context.lookup_value("keys", key) or key
            # Recursively translate the value
            translated[new_key] = translate_keys(context, value)
        return translated
    elif isinstance(data, list):
        # Recursively translate each item in the list
        return [translate_keys(context, item) for item in data]
    else:
        # Base case: return primitive values as-is
        return data


def get_parliamentary_terms(edustajatoimet: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract all parliamentary terms as a list."""
    term = edustajatoimet.pop("parliamentary_term")
    if isinstance(term, dict):
        return [term]
    elif isinstance(term, list):
        return term
    else:
        return []


def get_party(pep: dict[str, Any]) -> str:
    parliamentary_groups = pep.pop("parties")
    current_group = parliamentary_groups.pop("current_party")
    return cast(str, current_group.pop("name"))  # noqa: F821


def crawl(context: Context) -> None:
    data: list[dict[str, Any]] = context.fetch_json(context.data_url, cache_days=5)
    for item in data:
        pep_url = PEP_URL.format(person_id=item.pop("hetekaId"))
        raw_data = context.fetch_json(pep_url, cache_days=5)
        translated_data: dict[str, Any] = translate_keys(context, raw_data)
        pep_data: dict[str, Any] = translated_data.get("jsonNode", {}).get("person", {})

        first_name = pep_data.pop("first_name")
        last_name = pep_data.pop("last_name")
        birth_year = pep_data.pop("birth_year")
        person_id = pep_data.pop("person_id")

        entity = context.make("Person")
        entity.id = context.make_id(person_id, first_name, last_name, birth_year)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        entity.add("birthDate", birth_year)
        entity.add("birthPlace", pep_data.pop("birth_place", None))
        entity.add("gender", pep_data.pop("gender"), lang="eng")
        entity.add("position", pep_data.pop("occupation"))
        entity.add("address", pep_data.pop("current_municipality"))
        entity.add("sourceUrl", pep_url)
        entity.add("political", get_party(pep_data))
        entity.add("citizenship", "fi")

        position = h.make_position(
            context,
            name="Member of the Parliament of Finland",
            wikidata_id="Q17592486",
            country="fi",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        parliamentary_terms = pep_data.pop("parliamentary_terms", {})
        all_terms = get_parliamentary_terms(parliamentary_terms)

        for term in all_terms:
            start_date = term.pop("start_date")
            end_date = term.pop("end_date", None)

            occupancy = h.make_occupancy(
                context,
                person=entity,
                position=position,
                start_date=start_date,
                end_date=end_date,
                categorisation=categorisation,
            )
            if occupancy:
                context.emit(entity)
                context.emit(position)
                context.emit(occupancy)

        context.audit_data(pep_data, IGNORE)
