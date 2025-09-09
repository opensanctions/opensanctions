import string
import random
import hashlib
import base64

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from os import environ as env
from typing import Optional, List

from zavod import Context, helpers as h

# Note: These contain special characters, in testing use single quotes
# to make sure variables don't get interpolated by the shell.
WS_API_CLIENT_ID = env.get("OPENSANCTIONS_UA_WS_API_CLIENT_ID")
WS_API_KEY = env.get("OPENSANCTIONS_UA_WS_API_KEY")
# We keep these two secret because they were shared with us confidentially
WS_API_DOCS_URL = env.get("OPENSANCTIONS_UA_WS_API_DOCS_URL")
WS_API_BASE_URL = env.get("OPENSANCTIONS_UA_WS_API_BASE_URL")


class WSAPIDataType(str, Enum):
    PERSON = "person"
    ENTITY = "entity"
    VESSEL = "vessel"
    MANAGER = "manager"
    ROSTEC_STRUCTURE = "rostec_structure"


@dataclass(frozen=True)
class WSAPILink:
    endpoint: str
    type: WSAPIDataType
    program: str


LINKS: List[WSAPILink] = [
    WSAPILink(
        # child kidnappers
        "kidnappers/persons",
        WSAPIDataType.PERSON,
        "Persons involved in the deportation of Ukrainian children",
    ),
    WSAPILink(
        # child kidnappers
        "kidnappers/companies",
        WSAPIDataType.ENTITY,
        "Legal entities involved in the deportation of Ukrainian children",
    ),
    WSAPILink(
        # uav manufacturers
        "uav/companies",
        WSAPIDataType.ENTITY,
        "Legal entities involved in the production of UAVs",
    ),
    WSAPILink(
        # russian athletes
        "sport/persons",
        WSAPIDataType.PERSON,
        "Athletes and sports officials participating in Russian influence operations abroad",
    ),
    WSAPILink(
        # ships
        "transport/ships",
        WSAPIDataType.VESSEL,
        "Marine and Aircraft Vessels, Airports and Ports involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    ),
    WSAPILink(
        # ship management
        "transport/management",
        WSAPIDataType.MANAGER,
        "Management of ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    ),
    WSAPILink(
        # companies associated with ships
        "transport/companies",
        WSAPIDataType.ENTITY,
        "Companies associated with the ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    ),
    WSAPILink(
        # persons associated with ships
        "transport/persons",
        WSAPIDataType.PERSON,
        "Persons associated with the ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    ),
    WSAPILink(
        # captains
        "transport/captains",
        WSAPIDataType.PERSON,
        "Captains of ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    ),
    WSAPILink(
        # propagandists
        "propaganda/persons",
        WSAPIDataType.PERSON,
        "Persons involved in the dissemination of propaganda",
    ),
    WSAPILink(
        # executives of war
        "executives/persons",
        WSAPIDataType.PERSON,
        "Officials and entities controlling Russia’s military-industrial policy, defense orders, and wartime economy",
    ),
    WSAPILink(
        # stealers of heritage
        "stolen/persons",
        WSAPIDataType.PERSON,
        "Persons involved in the theft and destruction of Ukrainian cultural heritage",
    ),
    WSAPILink(
        # stealers of heritage
        "stolen/companies",
        WSAPIDataType.ENTITY,
        "Legal entities involved in the theft and destruction of Ukrainian cultural heritage",
    ),
    WSAPILink(
        # russian military-industrial complex
        "rostec/companies",
        WSAPIDataType.ENTITY,
        "Entities from Rostec’s core military holdings producing weapons for Russia’s war against Ukraine.",
    ),
    WSAPILink(
        # military component manufacturers
        "components/companies",
        WSAPIDataType.ENTITY,
        "Enterprises involved in the production and supply of military components and parts",
    ),
    WSAPILink(
        # factories
        "tools/companies",
        WSAPIDataType.ENTITY,
        "Legal entities involved in the production of military equipment and supplies",
    ),
    WSAPILink(
        # rostec structure
        "rostec/structure",
        WSAPIDataType.ROSTEC_STRUCTURE,
        "Rostec’s organizational structure and key entities",
    ),
]


def generate_token(cid: str, pkey: str) -> str:
    # 1. Create timestamp in ISO8601 (UTC)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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


def make_id(context: Context, entity_type: str, raw_id: str):
    return context.make_slug(entity_type, raw_id)


def split_dob_dod(raw_date):
    parts = [p.strip() for p in raw_date.split("-")]
    dob = parts[0] if parts and parts[0] else None
    dod = parts[1] if len(parts) > 1 and parts[1] else None
    return dob, dod


def crawl_ship_relation(
    context: Context,
    party_info,
    vessel_id_slug,
    rel_role: Optional[str] = None,
):
    company_id_raw = party_info.pop("id")
    start_date = party_info.pop("date")
    care_of_id_raw = party_info.pop("co_id", None)

    if rel_role == "owner":
        rel_schema, from_prop, to_prop = "Ownership", "owner", "asset"
    else:
        rel_schema, from_prop, to_prop = "UnknownLink", "subject", "object"

    emit_relation(
        context,
        subject_id=make_id(context, WSAPIDataType.MANAGER, company_id_raw),
        object_id=vessel_id_slug,
        rel_schema=rel_schema,
        rel_role=rel_role,
        from_prop=from_prop,
        to_prop=to_prop,
        start_date=start_date,
    )
    # e.g.
    # {
    #     "owner": {"id": "511", "date": "18.09.2023", "co_id": "512"},
    #     "commerce_manager": {"id": "512", "date": "22.11.2024", "co_id": None},
    #     "security_manager": {"id": "512", "date": "22.11.2024", "co_id": None},
    # }
    # Company 511 (subject) → "c/o" → Company 512 (object)
    if care_of_id_raw is not None:
        emit_relation(
            context,
            subject_id=make_id(context, WSAPIDataType.MANAGER, company_id_raw),
            object_id=make_id(context, WSAPIDataType.MANAGER, care_of_id_raw),
            rel_schema=rel_schema,
            rel_role="c/o",
            from_prop=from_prop,
            to_prop=to_prop,
        )

    context.audit_data(party_info)


def emit_relation(
    context: Context,
    *,
    subject_id,
    object_id,
    rel_schema: str = "UnknownLink",
    rel_role: Optional[str] = None,
    from_prop: str = "subject",
    to_prop: str = "object",
    start_date: Optional[str] = None,
):
    relation = context.make(rel_schema)
    relation.id = context.make_id(
        object_id, rel_role, subject_id, start_date, rel_schema
    )
    relation.add(from_prop, subject_id)
    relation.add(to_prop, object_id)
    relation.add("role", rel_role)
    h.apply_date(relation, "startDate", start_date)
    context.emit(relation)


def crawl_person(context: Context, person_data, program, endpoint, entity_type: str):
    birth_date = person_data.pop("date_bd")
    death_date = person_data.pop("date_death", None)
    if "- " in birth_date:
        birth_date, death_date = split_dob_dod(birth_date)

    person = context.make("Person")
    person.id = make_id(context, entity_type, person_data.pop("id"))
    person.add("name", person_data.pop("name_en"), lang="eng")
    person.add("name", person_data.pop("name_uk"), lang="ukr")
    person.add("name", person_data.pop("name_ru"), lang="rus")
    person.add("citizenship", person_data.pop("citizenships", None))
    person.add("taxNumber", person_data.pop("itn"))
    person.add("position", person_data.pop("positions", None))
    person.add("position", person_data.pop("position", None))
    person.add("position", person_data.pop("positions_main", None))
    person.add("position", person_data.pop("positions_other", None))
    h.apply_date(person, "birthDate", birth_date)
    h.apply_date(person, "deathDate", death_date)
    person.add("topics", "poi")
    person.add("birthPlace", person_data.pop("city_bd", None))

    sanction = h.make_sanction(context, person)
    sanction.add("reason", person_data.pop("reason", None))
    sanction.add("sourceUrl", person_data.pop("links", None))
    sanction.add("program", program)

    context.emit(person)
    context.emit(sanction)

    # 'transport/persons' and 'transport/captains' endpoints provide a list of vessel ids associated with persons
    related_ships = person_data.pop("ships", None)
    if related_ships:
        for ship_id_raw in related_ships:
            role = "captain" if endpoint == "transport/captains" else None
            emit_relation(
                context,
                subject_id=person.id,
                object_id=make_id(context, WSAPIDataType.VESSEL, ship_id_raw),
                rel_role=role,
            )

    context.audit_data(
        person_data, ["sanctions", "documents", "category", "sport", "places", "photo"]
    )


def crawl_legal_entity(context: Context, company_data, program, entity_type: str):
    legal_entity = context.make("LegalEntity")
    legal_entity.id = make_id(context, entity_type, company_data.pop("id"))
    legal_entity.add("name", company_data.pop("name"))
    name_abbr = company_data.pop("short")
    # If it's longer, it's usually just a little shortened version of name, not an abbreviation
    if len(name_abbr) < 11:
        legal_entity.add("alias", name_abbr)
    else:
        legal_entity.add("name", name_abbr)
    legal_entity.add("ogrnCode", company_data.pop("reg"))
    legal_entity.add("address", company_data.pop("address"))
    legal_entity.add("country", company_data.pop("country"))
    legal_entity.add("innCode", company_data.pop("itn"))
    legal_entity.add("topics", "poi")
    imo = company_data.pop("imo", None)
    if imo:
        legal_entity.add_cast("Company", "imoNumber", imo)

    sanction = h.make_sanction(context, legal_entity)
    sanction.add("reason", company_data.pop("reason"))
    sanction.add("sourceUrl", company_data.pop("links"))
    sanction.add("sourceUrl", company_data.pop("documents", None))
    sanction.add("program", program)

    context.emit(legal_entity)
    context.emit(sanction)

    # 'transport/companies' endpoint provides a list of vessel ids associated with legal entities
    related_ships = company_data.pop("ships", None)
    if related_ships:
        for ship_id_raw in related_ships:
            emit_relation(
                context,
                subject_id=legal_entity.id,
                object_id=make_id(context, WSAPIDataType.VESSEL, ship_id_raw),
            )

    context.audit_data(
        company_data,
        ["sanctions", "products", "rel_companies", "tools", "places", "logo"],
    )


def crawl_manager(context: Context, management_data, program, entity_type: str):
    manager = context.make("Company")
    manager.id = make_id(context, entity_type, management_data.pop("id"))
    manager.add("name", management_data.pop("name"))
    # We null falsy names via the lookups and set the topic once again here
    # not to emit empty entities
    if not manager.get("name"):
        manager.add("topics", "poi")
        context.emit(manager)
    manager.add("country", management_data.pop("country"))
    manager.add("imoNumber", management_data.pop("imo"))
    manager.add("topics", "poi")
    context.emit(manager)
    sanction = h.make_sanction(context, manager)
    sanction.add("program", program)
    context.emit(sanction)
    context.audit_data(management_data)


def crawl_vessel(context: Context, vessel_data, program, entity_type: str):
    vessel = context.make("Vessel")
    vessel.id = make_id(context, entity_type, vessel_data.pop("id"))
    vessel.add("name", vessel_data.pop("name"))
    vessel.add("imoNumber", vessel_data.pop("imo"))
    vessel.add("type", vessel_data.pop("type"))
    vessel.add("description", vessel_data.pop("info"))
    vessel.add("callSign", vessel_data.pop("callsign"))
    vessel.add("flag", vessel_data.pop("flag"))
    vessel.add("mmsi", vessel_data.pop("mmsi"))
    vessel.add("buildDate", vessel_data.pop("year"))
    vessel.add("grossRegisteredTonnage", vessel_data.pop("weight"))
    vessel.add("deadweightTonnage", vessel_data.pop("dwt"))
    old_data = vessel_data.pop("old_data", [])
    for item in old_data:
        vessel.add("previousName", item.pop("name"))
        vessel.add("pastFlags", item.pop("flag"))
    vessel.add("topics", "poi")
    if vessel_data.pop("is_shadow"):
        vessel.add("topics", "mare.shadow")

    sanction = h.make_sanction(context, vessel)
    sanction.add("program", program)
    sanction.add("sourceUrl", vessel_data.pop("links"))

    context.emit(vessel)
    context.emit(sanction)

    # 'role' is a field in the API response that we're matching on
    for role in ["commerce_manager", "security_manager", "owner"]:
        party_info = vessel_data.pop(role, None)
        if not party_info:
            continue
        crawl_ship_relation(context, party_info, vessel.id, role)

    pi_club_info = vessel_data.pop("pi_club", None)
    if pi_club_info:
        for club in pi_club_info:
            pi_club = context.make("Organization")
            pi_club.id = make_id(context, "organization", club.pop("id"))
            pi_club.add("name", club.pop("name"))
            context.emit(pi_club)
            emit_relation(
                context, subject_id=pi_club.id, object_id=vessel.id, rel_role="P&I Club"
            )

    context.audit_data(
        vessel_data,
        [
            "length",
            "shipyard",
            "captains",
            "false_data",
            "transponder_off",
            "russ_ports",
            "ports",
            "shadow_group",
            "sanctions_regime",
            "categories",
            "sanctions_lifted",
            "s_companies",
            "s_persons",
            "documents",
            "sanctions",
            "photo",
        ],
    )


def crawl_rostec_structure(context: Context, structure_data, entity_type: str):
    company_id = structure_data.pop("company_id")
    parent_id = structure_data.pop("parent_id")
    if parent_id and company_id:
        emit_relation(
            context,
            subject_id=make_id(context, entity_type, parent_id),
            object_id=make_id(context, entity_type, company_id),
            rel_schema="Ownership",
            rel_role="subsidiary of",
            from_prop="owner",
            to_prop="asset",
        )


def check_updates(context: Context):
    doc = context.fetch_html(WS_API_DOCS_URL)
    # Have any new sections been added?
    change_log = doc.xpath(".//main[@class='relative']")
    assert len(change_log) == 1, change_log
    h.assert_dom_hash(change_log[0], "18328f23fa487338c88a408674d1c4682a1f32c6")
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


def crawl(context: Context):
    check_updates(context)

    for link in LINKS:
        token = generate_token(WS_API_CLIENT_ID, WS_API_KEY)
        headers = {"Authorization": token}

        url = f"{WS_API_BASE_URL}{link.endpoint}"
        response = context.fetch_json(url, headers=headers, cache_days=1)
        if not response or response.get("code") != 0:
            context.log.error("No valid data to parse", url=url, response=response)
            continue
        data = response.get("data")
        for entity_details in data:
            if link.type is WSAPIDataType.PERSON:
                crawl_person(
                    context,
                    entity_details,
                    link.program,
                    link.endpoint,
                    WSAPIDataType.PERSON,
                )
            elif link.type is WSAPIDataType.ENTITY:
                crawl_legal_entity(
                    context, entity_details, link.program, WSAPIDataType.ENTITY
                )
            elif link.type is WSAPIDataType.VESSEL:
                crawl_vessel(
                    context, entity_details, link.program, WSAPIDataType.VESSEL
                )
            elif link.type is WSAPIDataType.MANAGER:
                crawl_manager(
                    context, entity_details, link.program, WSAPIDataType.MANAGER
                )
            elif link.type is WSAPIDataType.ROSTEC_STRUCTURE:
                crawl_rostec_structure(context, entity_details, WSAPIDataType.ENTITY)
            else:
                context.log.warn(f"Unknown data type: {link.type}")
