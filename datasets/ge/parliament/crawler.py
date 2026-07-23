from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The API needs a browser User-Agent (else 403) and both an Accept and Accept-Language
# header (else HTTP 500); Accept-Language "en" yields Latin-script (transliterated) names.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en",
}

# gender.id in the API payload.
GENDERS = {1: "male", 2: "female"}


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Georgia",
        country="ge",
        wikidata_id="Q21290878",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    payload = context.fetch_json(context.data_url, headers=HEADERS, cache_days=1)
    members: list[dict[str, Any]] = payload["data"]
    if not members:
        raise ValueError("Georgia deputies API returned no members")

    for member in members:
        first_name = (member.get("firstName") or "").strip().title()
        family_name = (member.get("familyName") or "").strip().title()
        assert first_name or family_name, f"Member without a name: {member.get('id')}"

        person = context.make("Person")
        person.id = context.make_slug(str(member["id"]))
        h.apply_name(person, first_name=first_name, last_name=family_name, lang="eng")
        gender = member.get("gender") or {}
        gender_id = gender.get("id")
        if isinstance(gender_id, int):
            person.add("gender", GENDERS.get(gender_id))
        h.apply_date(person, "birthDate", member.get("dob"))
        person.add("birthPlace", member.get("birthPlace"), lang="eng")
        # Candidates for Parliament must be citizens of Georgia (Constitution of Georgia,
        # Article 37(4)). https://www.constituteproject.org/constitution/Georgia_2018
        person.add("citizenship", "ge")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
