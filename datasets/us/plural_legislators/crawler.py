import re
import os
from typing import Any, Dict
from zipfile import ZipFile
from rigour.mime.types import ZIP
from yaml import safe_load
from urllib.parse import urlencode

from zavod.context import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

API_KEY = os.environ.get("OPENSANCTIONS_PLURAL_API_KEY")
REGEX_PATH = re.compile(
    "^people-main/data/(?P<jurisdiction>[a-z]{2})/(?P<body>legislature|executive|retired)"
)
REGEX_JURISDICTION = re.compile(
    ".+/(?P<type>state|district|territory):(?P<code>[a-z]{2})(?P<place>/(place|county):\w+)?(/government)?$"
)


def make_source_url(id: str) -> str:
    return f'https://pluralpolicy.com/app/person/{id.replace("ocd-person/", "")}'


def crawl_person(context, jurisdictions, house_positions, data: dict[str, Any]):
    if data.pop("death_date", None):
        return
    person = context.make("Person")
    source_id = data.pop("id")
    person.id = context.make_id(source_id)
    person.add("sourceUrl", make_source_url(source_id))
    person.add("country", "us")
    person.add("name", data.pop("name"))
    other_names = [n["name"] for n in data.pop("other_names", [])]
    person.add("alias", other_names)
    person.add("firstName", data.pop("given_name", None))
    person.add("lastName", data.pop("family_name", None))
    person.add("middleName", data.pop("middle_name", None))
    person.add("nameSuffix", data.pop("suffix", None))
    person.add("gender", data.pop("gender", None))
    person.add("birthDate", data.pop("birth_date", None))
    person.add("description", data.pop("biography", None))
    email = data.pop("email", None)
    if email is not None:
        person.add("email", email.strip("."))
    for party in data.pop("party", []):
        person.add("political", party.get("name", None))
    extras = data.pop("extras", {})
    person.add("gender", extras.pop("gender", None))
    person.add("title", extras.pop("title", None))
    homepages = [
        link["url"]
        for link in data.pop("links", [])
        if link.get("note", None) == "homepage"
    ]
    person.add("website", homepages)

    pep_entities = []
    for role in data.pop("roles"):
        role_type = role.get("type")
        role_match = REGEX_JURISDICTION.match(role["jurisdiction"])
        if role_match:
            if role_match.group("place"):
                # skip local government
                continue
        else:
            context.log.warning(
                "No match for jurisdiction.", jurisdiction=role["jurisdiction"]
            )
            continue

        jurisdiction_code = role_match.group("code")
        jurisdiction_name = jurisdictions[jurisdiction_code]

        position_key = (jurisdiction_code, role_type)
        position_name = house_positions.get(position_key, None)
        if position_name is None:
            res = context.lookup("position", role_type)
            if res:
                if res.position_prefix:
                    position_name = f"{res.position_prefix} of {jurisdiction_name}"
                else:
                    # Explicitly marked positions we're not interested in
                    context.log.info(
                        "Skipping position.",
                        source_id=source_id,
                        position_key=position_key,
                    )
                    continue
        if position_name is None:
            context.log.warning(
                "Unknown position",
                position_key=position_key,
                jurisdiction=role["jurisdiction"],
            )
            continue

        position = h.make_position(
            context, position_name, country="us", subnational_area=jurisdiction_name
        )
        categorisation = categorise(context, position, True)
        if not categorisation.is_pep:
            return
        start_date = role.get("start_date", None)
        end_date = role.get("end_date", None)
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            True,
            start_date=str(start_date) if start_date else None,
            end_date=str(end_date) if end_date else None,
            categorisation=categorisation,
        )
        if occupancy:
            pep_entities.append(position)
            pep_entities.append(occupancy)

    context.audit_data(
        data,
        ignore=[
            "image",
            "party",
            "other_identifiers",
            "sources",
            "offices",
            "extras",
            "ids",
        ],
    )

    if pep_entities:
        context.emit(person)
    for entity in pep_entities:
        context.emit(entity)


def crawl_jurisdictions(context: Context):
    if API_KEY is None:
        raise ValueError("No OPENSANCTIONS_PLURAL_API_KEY key set for OpenStates")
    jurisdictions = {}
    house_positions = {}
    headers = {"x-api-key": API_KEY}
    query: Dict[str, Any] = {
        "page": 1,
        "classification": "state",
        "include": "organizations",
    }
    while True:
        url = f"https://v3.openstates.org/jurisdictions?{urlencode(query)}"
        result = context.fetch_json(url, headers=headers, cache_days=1)
        for jurisdiction in result["results"]:
            name = jurisdiction["name"]
            match = REGEX_JURISDICTION.match(jurisdiction.get("division_id"))
            if not match:
                print(jurisdiction.get("division_id"))
                continue
            code = match.group("code")
            jurisdictions[code] = name

            for org in jurisdiction["organizations"]:
                type = org["classification"]
                if type == "legislature":
                    house_positions[(code, type)] = f'Member of the {org["name"]}'
                if type == "upper":
                    house_positions[(code, type)] = (
                        f'Member of the {name} {org["name"]}'
                    )
                if type == "lower":
                    representative = org["districts"][0]["role"]
                    house_positions[(code, type)] = (
                        f'Member of the {name} {org["name"]} of {representative}s'
                    )

        if query.get("page") == result.get("pagination").get("max_page", None):
            break
        query["page"] += 1
    return jurisdictions, house_positions


def crawl(context: Context):
    jurisdictions, house_positions = crawl_jurisdictions(context)
    path = context.fetch_resource("source.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    with ZipFile(path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            match = REGEX_PATH.match(member.filename)
            if match and match.group("jurisdiction") != "us":
                # context.log.info(f"Crawling {member.filename}")
                with archive.open(member) as filestream:
                    crawl_person(
                        context,
                        jurisdictions,
                        house_positions,
                        safe_load(filestream),
                    )
