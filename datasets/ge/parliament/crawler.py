from typing import Any
from lxml import html

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    first_name = member.pop("firstName").strip()
    family_name = member.pop("familyName").strip()
    father_name = member.pop("fatherName")

    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    h.apply_name(person, first_name=first_name, last_name=family_name, lang="eng")
    if father_name is not None:
        person.add("fatherName", father_name.strip(), lang="eng")
    person.add("gender", str(member.pop("gender")["id"]))

    h.apply_date(person, "birthDate", member.pop("dob"))
    person.add("birthPlace", member.pop("birthPlace"), lang="eng")
    person.add("notes", member.pop("declaration"))  # link to declaration pdf

    # biography is an HTML fragment; extract the text of each paragraph and list
    # item (decoding entities and stripping tags) into clean plain text.
    biography = member.pop("biography")
    if biography:
        doc = html.fromstring(f"<div>{biography}</div>")
        paragraphs = [
            h.element_text(el) for el in h.xpath_elements(doc, ".//p | .//li")
        ]
        person.add("biography", "\n".join(p for p in paragraphs if p))

    contact_info = member.pop("contactInfo")
    for email in contact_info["emails"]:
        person.add("email", email)
    for phone in contact_info["phones"]:
        person.add("phone", phone)

    # Candidates for Parliament must be citizens of Georgia (Constitution of Georgia,
    # Article 37(4)). https://www.constituteproject.org/constitution/Georgia_2018
    person.add("citizenship", "ge")

    context.audit_data(
        member,
        ignore=[
            "avatar",
            "bureau",
            "colleagueStatus",
            "fullName",  # often None
            "image",
            "local_avatar",
            "mp",
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
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Accept-Language "en" yields Latin-script (transliterated) names; without an
    # Accept header the API returns HTTP 500. The browser User-Agent that avoids a
    # 403 is configured via http.user_agent in the dataset metadata.
    payload = context.fetch_json(
        context.data_url,
        headers={"Accept": "application/json", "Accept-Language": "en"},
        cache_days=1,
    )
    for member in payload["data"]:
        crawl_member(context, member, position, categorisation)
