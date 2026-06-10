import base64
import hashlib
import random
import string
import json

from dataclasses import dataclass
from enum import Enum
from normality import squash_spaces
from os import environ as env
from typing import Any, Dict, Optional, List
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_json, fetch, fetch_html, ZyteAPIRequest

# Note: These contain special characters, in testing use single quotes
# to make sure variables don't get interpolated by the shell.
# export OPENSANCTIONS_UA_WS_API_CLIENT_ID=$(op read op://$OS_ENGINEERING_VAULT_ID/os_env/ua_ws_api_client_id)
# export OPENSANCTIONS_UA_WS_API_KEY=$(op read op://$OS_ENGINEERING_VAULT_ID/os_env/ua_ws_api_key)
# export OPENSANCTIONS_UA_WS_API_DOCS_URL=$(op read op://$OS_ENGINEERING_VAULT_ID/os_env/ua_ws_api_docs_url)
# export OPENSANCTIONS_UA_WS_API_BASE_URL=$(op read op://$OS_ENGINEERING_VAULT_ID/os_env/ua_ws_api_base_url)
WS_API_CLIENT_ID = env["OPENSANCTIONS_UA_WS_API_CLIENT_ID"]
WS_API_KEY = env["OPENSANCTIONS_UA_WS_API_KEY"]
# We keep these two secret because they were shared with us confidentially
WS_API_DOCS_URL = env["OPENSANCTIONS_UA_WS_API_DOCS_URL"]
WS_API_BASE_URL = env["OPENSANCTIONS_UA_WS_API_BASE_URL"]
SLEEP = 10

SPLITS = [" / ", "\r\n", "/"]
NAMES_LANG_MAP = {
    "name_en": "eng",
    "name_uk": "ukr",
    "name_ru": "rus",
}
RESPONSE_CODES = {
    0: "successful request",
    1: "missing Authorization header",
    2: "base64 string decoding error",
    3: "invalid Client ID",
    4: "digital signature verification error",
    5: "invalid or expired timestamp",
    6: "token reuse",
    7: "invalid URL",
    8: "access denied for IP address",
    255: "internal error",
}


class WSAPIDataType(str, Enum):
    PERSON = "person"
    ENTITY = "entity"
    ROSTEC_STRUCTURE = "rostec_structure"


@dataclass(frozen=True)
class WSAPILink:
    endpoint: str
    type: WSAPIDataType
    program_key: Optional[str] = None
    # topic=None skips adding a topics property (used for sanctions lists that aren't POI-tagged)
    topic: Optional[str] = "poi"
    # Defaults are rus entity codes; override for non-rus entities (e.g. int sanctions lists)
    reg_prop: str = "ogrnCode"
    itn_prop: str = "innCode"


# NOTE: Vessel data (transport/ships, transport/management, transport/companies,
# transport/persons, transport/captains) is no longer sourced from the API.
# The API contains outdated vessel information; vessels will be covered by
# crawling the website directly.
LINKS: List[WSAPILink] = [
    WSAPILink(
        # child kidnappers
        "kidnappers/persons",
        WSAPIDataType.PERSON,
        "UA-WS-KIDNAPPERS",
    ),
    WSAPILink(
        # child kidnappers
        "kidnappers/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-KIDNAPPERS",
    ),
    WSAPILink(
        # uav manufacturers
        "uav/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-UAVS",
    ),
    WSAPILink(
        # russian athletes
        "sport/persons",
        WSAPIDataType.PERSON,
        "UA-WS-ATHLETES",
    ),
    WSAPILink(
        # propagandists
        "propaganda/persons",
        WSAPIDataType.PERSON,
        "UA-WS-PROPAGANDISTS",
    ),
    WSAPILink(
        # executives of war
        "executives/persons",
        WSAPIDataType.PERSON,
        "UA-WS-EXECUTIVES",
    ),
    WSAPILink(
        # stealers of heritage
        "stolen/persons",
        WSAPIDataType.PERSON,
        "UA-WS-STEALERS",
    ),
    WSAPILink(
        # stealers of heritage
        "stolen/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-STEALERS",
    ),
    WSAPILink(
        # russian military-industrial complex
        "rostec/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-MILIND",
    ),
    WSAPILink(
        # military component manufacturers
        "components/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-MILIND",
    ),
    WSAPILink(
        # factories
        "tools/companies",
        WSAPIDataType.ENTITY,
        "UA-WS-MILIND",
    ),
    WSAPILink(
        # rostec structure
        "rostec/structure",
        WSAPIDataType.ROSTEC_STRUCTURE,
        "UA-WS-MILIND",
    ),
    WSAPILink(
        # partner's sanctions lists - legal entities
        "sanctions/companies",
        WSAPIDataType.ENTITY,
        topic=None,
        reg_prop="registrationNumber",
        itn_prop="taxNumber",
    ),
    WSAPILink(
        # partner's sanctions lists - individuals
        "sanctions/persons",
        WSAPIDataType.PERSON,
        topic=None,
    ),
]


def generate_token(context: Context, cid: str, pkey: str) -> str:
    # Request with a timestamp that is more than 15 seconds off
    # from the our server time will not be processed.
    # Zyte because cloudflare is blocking us possibly based on IP reputation
    # - I can't reproduce the block from our GCP jump host.
    timestamp = fetch_json(context, f"{WS_API_BASE_URL}/time")["server_time"]
    context.log.debug("Server timestamp", timestamp_=timestamp)
    # 2. Generate server instance ID (exactly 2 characters)
    sid = "".join(random.choices(string.ascii_letters + string.digits, k=2))
    # 3. Create signature = sha256(cid + sid + timestamp + pkey), lowercase hex
    signature_input = f"{cid}{sid}{timestamp}{pkey}"
    signature = hashlib.sha256(signature_input.encode()).hexdigest().lower()
    # 4. Build raw token string = signature + cid + sid + timestamp
    raw_token = f"{signature}{cid}{sid}{timestamp}"
    # 5. Base64 encode
    token = base64.b64encode(raw_token.encode()).decode()
    return token


def apply_names(context: Context, person: Entity, person_data: Dict[str, str]) -> None:
    for key, lang in NAMES_LANG_MAP.items():
        raw_name = person_data.pop(key)
        h.apply_reviewed_name_string(context, person, string=raw_name, lang=lang)


def make_id(context: Context, entity_type: str, raw_id: str) -> str | None:
    return context.make_slug(entity_type, raw_id)


def split_dob_dod(raw_date: str) -> tuple[str | None, str | None]:
    parts = [p.strip() for p in raw_date.split("-")]
    dob = parts[0] if parts and parts[0] else None
    dod = parts[1] if len(parts) > 1 and parts[1] else None
    return dob, dod


def fetch_endpoint(context: Context, url: str, max_retries: int = 4) -> dict[str, Any]:
    # Retry on code 5 (invalid or expired timestamp) — Zyte can be slow to route
    # the request and the 15-second timestamp window expires in transit.
    for attempt in range(max_retries + 1):
        token = generate_token(context, WS_API_CLIENT_ID, WS_API_KEY)
        zyte_result = fetch(
            context,
            ZyteAPIRequest(
                url=url,
                headers={
                    "Accept": "application/json",
                    "Authorization": token,
                },
            ),
            cache_days=1,
        )
        response: dict[str, Any] = json.loads(zyte_result.response_text)
        if response and response.get("code") == 0:
            return response
        context.cache.delete(zyte_result.cache_fingerprint)
        error_code = response.get("code") if response else None
        if error_code != 5 or attempt >= max_retries:
            error = RESPONSE_CODES.get(error_code) if error_code is not None else None
            raise Exception(
                f"Failed to fetch data for {url} error={error} response={response}"
            )
        # Info level because we know this happens regularly but isn't normally surprising.
        context.log.info("Retrying after timestamp error", url=url, attempt=attempt + 1)
    raise Exception("Too many timestamp errors.")


def emit_relation(
    context: Context,
    *,
    subject_id: str | None,
    object_id: str | None,
    rel_schema: str = "UnknownLink",
    rel_role: Optional[str] = None,
    from_prop: str = "subject",
    to_prop: str = "object",
    start_date: Optional[str] = None,
) -> None:
    relation = context.make(rel_schema)
    relation.id = context.make_id(
        object_id, rel_role, subject_id, start_date, rel_schema
    )
    relation.add(from_prop, subject_id)
    relation.add(to_prop, object_id)
    if rel_role is not None:
        relation.add("role", rel_role.replace("_", " "))
    h.apply_date(relation, "startDate", start_date)
    context.emit(relation)


def crawl_person(
    context: Context,
    person_data: Dict[str, Any],
    *,
    program_key: str,
    endpoint: str,
    source_url: str,
    topic: Optional[str] = "poi",
) -> None:
    birth_date = person_data.pop("date_bd")
    death_date = person_data.pop("date_death", None)
    if "- " in birth_date:
        birth_date, death_date = split_dob_dod(birth_date)

    person = context.make("Person")
    person.id = make_id(context, WSAPIDataType.PERSON, person_data.pop("id"))
    apply_names(context, person, person_data)
    person.add("citizenship", person_data.pop("citizenships", None))
    person.add("taxNumber", h.multi_split(person_data.pop("itn"), " / "))
    for key in ("positions", "position", "positions_main", "positions_other"):
        position = person_data.pop(key, None)
        for p in h.multi_split(position, SPLITS):
            person.add("position", squash_spaces(p))
    # Source API sometimes encodes multiple or uncertain dates in one field
    # (e.g. "1965; 1966", "1975 or 1976", "Jan 1980 to Mar 1980")
    for birth_date in h.multi_split(birth_date, [";", ", ", " to ", " or "]):
        h.apply_date(person, "birthDate", birth_date)
    h.apply_date(person, "deathDate", death_date)
    if topic:
        person.add("topics", topic)
    person.add("birthPlace", person_data.pop("city_bd", None))
    person.add("sourceUrl", source_url)

    sanction = h.make_sanction(
        context, person, key=program_key, program_key=program_key
    )
    sanction.set("programUrl", source_url)
    sanction.add("reason", person_data.pop("reason", None))
    sanction.add("sourceUrl", person_data.pop("links", None))

    context.emit(person)
    context.emit(sanction)

    context.audit_data(
        person_data, ["sanctions", "documents", "category", "sport", "places", "photo"]
    )


def crawl_legal_entity(
    context: Context,
    company_data: Dict[str, str],
    *,
    program_key: Optional[str],
    source_url: str,
    topic: Optional[str] = "poi",
    reg_prop: str = "ogrnCode",
    itn_prop: str = "innCode",
) -> None:
    legal_entity = context.make("LegalEntity")
    legal_entity.id = make_id(context, WSAPIDataType.ENTITY, company_data.pop("id"))
    legal_entity.add("name", h.multi_split(company_data.pop("name"), [" / "]))
    name_abbr = h.multi_split(company_data.pop("short"), [" / "])
    # If it's longer, it's usually just a little shortened version of name, not an abbreviation
    for alias in name_abbr:
        if len(alias) < 11:
            legal_entity.add("alias", alias)
        else:
            legal_entity.add("name", alias)
    legal_entity.add(reg_prop, company_data.pop("reg"))
    legal_entity.add("address", company_data.pop("address"))
    legal_entity.add("country", company_data.pop("country"))
    legal_entity.add(itn_prop, company_data.pop("itn"))
    if topic:
        legal_entity.add("topics", topic)
    legal_entity.add("sourceUrl", source_url)
    imo = company_data.pop("imo", None)
    if imo:
        legal_entity.add_cast("Company", "imoNumber", imo)

    sanction = h.make_sanction(
        context, legal_entity, key=program_key, program_key=program_key
    )
    sanction.add("reason", company_data.pop("reason"))
    sanction.set("programUrl", source_url)
    sanction.add("sourceUrl", company_data.pop("links"))
    sanction.add("sourceUrl", company_data.pop("documents", None))

    context.emit(legal_entity)
    context.emit(sanction)

    context.audit_data(
        company_data,
        ["sanctions", "products", "rel_companies", "tools", "places", "logo"],
    )


def crawl_rostec_structure(context: Context, structure_data: Dict[str, Any]) -> None:
    company_id = structure_data.pop("company_id")
    parent_id = structure_data.pop("parent_id")
    if parent_id and company_id:
        emit_relation(
            context,
            subject_id=make_id(context, WSAPIDataType.ENTITY, parent_id),
            object_id=make_id(context, WSAPIDataType.ENTITY, company_id),
            rel_schema="Ownership",
            rel_role="subsidiary of",
            from_prop="owner",
            to_prop="asset",
        )


def check_updates(context: Context) -> None:
    # NOTE: When debugging, uncomment the logging below ONLY in local development.
    # Do not enable in production or commit uncommented to avoid leaking
    # the API docs key in the logs.
    try:
        doc = fetch_html(context, WS_API_DOCS_URL, ".//h1[text() = 'Changelog']")
    except Exception:  #  as e:
        context.log.warn(
            "Failed to fetch API documentation",
            # url=WS_API_DOCS_URL,
            # error=str(e),
        )
        return
    # Have any new sections been added?
    change_log = h.xpath_element(doc, ".//main[@class='relative']")
    h.assert_dom_hash(change_log, "e58955c7ef5c543cd4c2ae8975eb326287e09023")
    # Existing sections from the API documentation sidebar
    #
    # Kidnappers:
    # - kidnappers persons
    # - kidnappers companies
    #
    # Components in weapons:
    # - components list
    # - weapon parts
    # - related legal entities (suppliers, importers, foreign suppliers, etc.)
    #
    # Instruments of war:
    # - equipment list
    # - factories of the location
    #
    # Marine and aircraft vessels:
    # - ships list
    # - related legal entities
    # - related individuals
    # - captains
    # - sea ports
    # - ships categories
    # - ships management (owners, commerce managers, security managers)
    #
    # Stolen Heritage:
    # - objects list
    # - involved legal entities
    # - involved individuals
    # - places of the incidents
    #
    # Partner`s sanctions lists:
    # - sanctions directory
    # - lifted sanctions directory
    # - legal entities
    # - individuals
    #
    # Champions of terror:
    # - individuals
    #
    # Kremlin Mouthpieces:
    # - individuals
    #
    # Executives of War:
    # - individuals
    # - structure
    #
    # UAV manufacturers:
    # - UAV list
    # - manufacturers
    #
    # Rostec:
    # - legal entities
    # - structure


def crawl(context: Context) -> None:
    check_updates(context)

    for link in LINKS:
        url = f"{WS_API_BASE_URL}/v1/{link.endpoint}"
        # Zyte because cloudflare is blocking us possibly based on IP reputation
        # - I can't reproduce the block from our GCP jump host.
        response = fetch_endpoint(context, url)

        data = response.get("data")
        for entity_details in data:
            # Construct entity specific page URL: {base_url}/{endpoint}/{entity_id}
            entity_id = entity_details.get("id")
            source_url = urljoin(context.data_url, f"{link.endpoint}/{entity_id}")

            if link.type is WSAPIDataType.PERSON:
                crawl_person(
                    context,
                    entity_details,
                    program_key=link.program_key,
                    endpoint=link.endpoint,
                    source_url=source_url,
                    topic=link.topic,
                )
            elif link.type is WSAPIDataType.ENTITY:
                crawl_legal_entity(
                    context,
                    entity_details,
                    program_key=link.program_key,
                    source_url=source_url,
                    topic=link.topic,
                    reg_prop=link.reg_prop,
                    itn_prop=link.itn_prop,
                )
            elif link.type is WSAPIDataType.ROSTEC_STRUCTURE:
                crawl_rostec_structure(context, entity_details)
            else:
                context.log.warn(f"Unknown data type: {link.type}")
