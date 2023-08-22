from zipfile import ZipFile
from pantomime.types import ZIP
from yaml import safe_load
from typing import Any
import re
import os
from urllib.parse import urlencode

from zavod.context import Context
from zavod import helpers as h

API_KEY = os.environ.get("OPENSANCTIONS_PLURAL_API_KEY")
REGEX_PATH = re.compile(
    "^people-main/data/(?P<jurisdiction>[a-z]{2})/(?P<body>legislature|executive|retired)"
)
REGEX_JURISDICTION = re.compile(".+/(?P<type>state|district|territory):(?P<code>[a-z]{2})(?P<place>/place:\w+)?(/government)?$")


def crawl_person(context, jurisdictions, house_positions, data: dict[str, Any]):
    for role in data.pop("roles"):
        role_type = role.get("type")

        role_match = REGEX_JURISDICTION.match(role["jurisdiction"])
        if role_match:
            if role_match.group("place"):
                # skip local government
                continue
        else:
            context.log.warning("No match for jurisdiction.", jurisdiction=role["jurisdiction"])
            continue

        position_key = (role_match.group("code"), role_type)
        position_name = house_positions.get(position_key, None)
        if position_name is None:
            res = context.lookup("position", role_type)
            if res and res.position_prefix:
                position_name = f'{res.position_prefix} of {jurisdictions[role_match.group("code")]}'

        if position_name is None:
            context.log.warning("Unknown position", position_key=position_key, jurisdiction=role["jurisdiction"])
            continue
        #print(
        #    "  ",
        #    position_name,
        #    {"start": role.get("start_date", None), "end": role.get("end_date", None)},
        #)

        # governor of {state}
        # member of the {state} senate
        # member of the {state} house of reps


def crawl_jurisdictions(context: Context):
    jurisdictions = {}
    house_positions = {}
    headers = {"x-api-key": API_KEY}
    query = {
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
                if org["classification"] == "upper":
                    house_positions[(code, "upper")] = f'Member of the {name} {org["name"]}'
                if org["classification"] == "lower":
                    representative = org["districts"][0]["role"]
                    house_positions[
                        (code, "lower")
                    ] = f'Member of the {name} {org["name"]} of {representative}s'

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
                with archive.open(member) as filestream:
                    crawl_person(
                        context,
                        jurisdictions,
                        house_positions,
                        safe_load(filestream),
                    )
