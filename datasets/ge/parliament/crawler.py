from typing import Any

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise

# The API needs a browser User-Agent (else 403) and both an Accept and Accept-Language
# header (else HTTP 500); Accept-Language "en" yields Latin-script (transliterated) names.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 (zavod; opensanctions.org)"
    ),
    "Accept": "application/json",
    "Accept-Language": "en",
}

# gender.id in the API payload.
GENDERS = {1: "male", 2: "female"}


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    source_id = member.pop("id")
    # The endpoint serves parliamentary "colleagues"; every row today is a sitting deputy
    # (mp=True). Fail loudly if a non-MP ever appears rather than emitting it as one.
    if member.pop("mp") is not True:
        raise ValueError(f"Non-MP colleague in deputies feed: {source_id}")

    first_name = (member.pop("firstName") or "").strip().title()
    family_name = (member.pop("familyName") or "").strip().title()
    if not (first_name or family_name):
        raise ValueError(f"Member without a name: {source_id}")

    person = context.make("Person")
    person.id = context.make_slug(str(source_id))
    h.apply_name(person, first_name=first_name, last_name=family_name, lang="eng")

    gender = member.pop("gender", None)
    if gender is not None:
        label = GENDERS.get(gender.get("id"))
        if label is None:
            raise ValueError(f"Unknown gender {gender!r} for member {source_id}")
        person.add("gender", label)

    h.apply_date(person, "birthDate", member.pop("dob"))
    person.add("birthPlace", member.pop("birthPlace"), lang="eng")
    # Candidates for Parliament must be citizens of Georgia (Constitution of Georgia,
    # Article 37(4)). https://www.constituteproject.org/constitution/Georgia_2018
    person.add("citizenship", "ge")

    # contactInfo carries phone numbers, which we deliberately never extract.
    context.audit_data(
        member,
        ignore=[
            "avatar",
            "biography",
            "bureau",
            "colleagueStatus",
            "contactInfo",
            "declaration",
            "fatherName",
            "fullName",
            "image",
            "local_avatar",
            "node",
            "position",
            "positionIndex",
            "supervisorId",
            "supervisorName",
            "supervisorNameEn",
            "voteCount",
        ],
    )

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


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
        crawl_member(context, member, position, categorisation)
