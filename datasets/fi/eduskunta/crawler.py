import json
from typing import Any, cast

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api

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
    "person_id",
]


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
    """Extract all parliamentary terms (current term and all previous ones) for the current MP."""
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
    _, _, _, path = zyte_api.fetch_resource(
        context, "data.json", f"{context.data_url}seating/"
    )
    with open(path, "r") as fh:
        data = json.load(fh)
    for item in data:
        pep_id = item.pop("hetekaId")
        _, _, _, raw_path = zyte_api.fetch_resource(
            context,
            f"item_{pep_id}.json",
            f"{context.data_url}memberofparliament/{pep_id}/fi",
        )
        with open(raw_path, "r") as fh:
            raw_data = json.load(fh)
        translated_data: dict[str, Any] = translate_keys(context, raw_data)
        pep_data: dict[str, Any] = translated_data.get("jsonNode", {}).get("person", {})

        first_name = pep_data.pop("first_name")
        last_name = pep_data.pop("last_name")
        birth_year = pep_data.pop("birth_year")

        entity = context.make("Person")
        entity.id = context.make_id(pep_id, first_name, last_name, birth_year)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        entity.add("birthDate", birth_year)
        entity.add("birthPlace", pep_data.pop("birth_place", None))
        entity.add("gender", pep_data.pop("gender"), lang="eng")
        entity.add("position", pep_data.pop("occupation"))
        entity.add("address", pep_data.pop("current_municipality"))
        entity.add("political", get_party(pep_data))
        entity.add(
            "sourceUrl",
            f"https://www.eduskunta.fi/SV/kansanedustajat/Sidor/{pep_id}.aspx",
        )
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
        # Past and current terms for the current Member of Parliament
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
