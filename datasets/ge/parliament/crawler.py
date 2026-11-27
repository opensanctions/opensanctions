import json
from typing import Any

from lxml import html
from rigour.mime.types import JSON

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    first_name = member.pop("firstName").strip()
    family_name = member.pop("familyName").strip()

    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    h.apply_name(person, first_name=first_name, last_name=family_name, lang="eng")
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
            "fatherName",
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
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21290878",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Cloudflare blocks direct requests to the API, so fetch through Zyte.
    # zyte_api.fetch_json can't send Accept-Language, which is needed here:
    # "en" yields Latin-script (transliterated) names. Without an Accept
    # header the API returns HTTP 500.
    _, _, _, text = zyte_api.fetch_text(
        context,
        context.data_url,
        headers={"Accept": JSON, "Accept-Language": "en"},
        expected_media_type=JSON,
        cache_days=1,
    )
    payload = json.loads(text)
    for member in payload["data"]:
        crawl_member(context, member, position, categorisation)
