import string
import random
import hashlib
import base64
from os import environ as env
from datetime import datetime, timezone

# from pprint import pprint

from zavod import Context, helpers as h

# Make sure variables don't get interpolated by the shell.
WS_CID = env.get("OPENSANCTIONS_WS_CID")
WS_API_KEY = env.get("OPENSANCTIONS_WS_API_KEY")


LINKS = [
    {  # child kidnappers
        "endpoint": "kidnappers/persons",
        "type": "person",
        "program": "Persons involved in the deportation of Ukrainian children",
    },
    {  # child kidnappers
        "endpoint": "kidnappers/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the deportation of Ukrainian children",
    },
    {  # uav manufacturers
        "endpoint": "uav/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the production of UAVs",
    },
    {  # russian athletes
        "endpoint": "sport/persons",
        "type": "person",
        "program": "Athletes and sports officials participating in Russian influence operations abroad",
    },
    {  # ships
        "endpoint": "transport/ships",
        "type": "vessel",
        "program": "Marine and Aircraft Vessels, Airports and Ports involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {  # ship management
        "endpoint": "transport/management",
        "type": "management",
        "program": "Management of ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {
        "endpoint": "transport/companies",
        "type": "legal_entity",
        "program": "Companies associated with the ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {
        "endpoint": "transport/persons",
        "type": "person",
        "program": "Persons associated with the ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {  # captains
        "endpoint": "transport/captains",
        "type": "person",
        "program": "Captains of ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {  # propagandists
        "endpoint": "propaganda/persons",
        "type": "person",
        "program": "Persons involved in the dissemination of propaganda",
    },
    {  # executives of war
        "endpoint": "executives/persons",
        "type": "person",
        "program": "Officials and entities controlling Russia’s military-industrial policy, defense orders, and wartime economy",
    },
    {  # stealers of heritage
        "endpoint": "stolen/persons",
        "type": "person",
        "program": "Persons involved in the theft and destruction of Ukrainian cultural heritage",
    },
    {  # stealers of heritage
        "endpoint": "stolen/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the theft and destruction of Ukrainian cultural heritage",
    },
    {  # russian military-industrial complex
        "endpoint": "rostec/companies",
        "type": "legal_entity",
        "program": "Entities from Rostec’s core military holdings producing weapons for Russia’s war against Ukraine.",
    },
    {  # military component manufacturers
        "endpoint": "components/companies",
        "type": "legal_entity",
        "program": "Enterprises involved in the production and supply of military components and parts",
    },
    {  # factories
        "endpoint": "tools/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the production of military equipment and supplies",
    },
]


def generate_token(cid: str, pkey: str) -> str:
    # 1. Create timestamp in ISO8601 (UTC)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # 2. Generate server instance ID (exactly 2 characters)
    sid = "".join(random.choices(string.ascii_letters + string.digits, k=2))
    # 3. Create signature = sha256(cid + sid + timestamp + pkey), lowercase hex
    signature_input = f"{cid}{sid}{timestamp}{pkey}"
    signature = hashlib.sha256(signature_input.encode("utf-8")).hexdigest().lower()
    # 4. Build raw token string = signature + cid + sid + timestamp
    raw_token = f"{signature}{cid}{sid}{timestamp}"
    # 5. Base64 encode
    token = base64.b64encode(raw_token.encode("utf-8")).decode("utf-8")
    return token


def emit_link(context: Context, ship_id, entity_id):
    vessel = context.make("Vessel")
    vessel.id = context.make_slug("vessel", ship_id)
    link = context.make("UnknownLink")
    link.id = context.make_id(ship_id, entity_id)
    link.add("subject", entity_id)
    link.add("object", vessel.id)
    context.emit(link)


def crawl_person(context: Context, person_data, program):
    id = person_data.pop("id")
    name_en = person_data.pop("name_en")
    name_uk = person_data.pop("name_uk")
    name_ru = person_data.pop("name_ru")
    positions = person_data.pop("positions", None)
    position = person_data.pop("position", None)
    dob = person_data.pop("date_bd")
    pob = person_data.pop("city_bd", None)
    links = person_data.pop("links", None)

    person = context.make("Person")
    person.id = context.make_slug("person", id)
    person.add("name", name_en, lang="eng")
    person.add("name", name_uk, lang="ukr")
    person.add("name", name_ru, lang="rus")
    person.add("citizenship", person_data.pop("citizenships", None))
    person.add("taxNumber", person_data.pop("itn"))
    person.add("sourceUrl", links)
    person.add("position", positions)
    person.add("position", position)
    person.add("position", person_data.pop("positions_main", None))
    person.add("position", person_data.pop("positions_other", None))
    h.apply_date(person, "birthDate", dob)
    h.apply_date(person, "deathDate", person_data.pop("date_death", None))
    person.add("topics", "poi")
    person.add("sourceUrl", links)
    person.add("sourceUrl", person_data.pop("photo"))
    person.add("birthPlace", pob)

    sanction = h.make_sanction(context, person)
    sanction.add("reason", person_data.pop("reason", None))
    sanction.add("sourceUrl", links)
    sanction.add("program", program)

    context.emit(person)
    context.emit(sanction)

    # 'transport/persons' and 'transport/captains' endpoints provide a list of vessel ids associated with persons
    related_ships = person_data.pop("ships", None)
    if related_ships:
        for ship_id in related_ships:
            emit_link(context, ship_id, person.id)

    context.audit_data(
        person_data, ["sanctions", "documents", "category", "sport", "places"]
    )


def crawl_legal_entity(context: Context, company_data, program):
    id = company_data.pop("id")
    name = company_data.pop("name")
    name_abbr = company_data.pop("short")
    reg_num = company_data.pop("reg")
    imo = company_data.pop("imo", None)

    legal_entity = context.make("LegalEntity")
    legal_entity.id = context.make_slug("entity", id)
    legal_entity.add("name", name)
    if len(name_abbr) < 11:
        legal_entity.add("alias", name_abbr)
    else:
        legal_entity.add("name", name_abbr)
    legal_entity.add("ogrnCode", reg_num)
    legal_entity.add("address", company_data.pop("address"))
    legal_entity.add("country", company_data.pop("country"))
    legal_entity.add("innCode", company_data.pop("itn"))
    legal_entity.add("sourceUrl", company_data.pop("links"))
    legal_entity.add("sourceUrl", company_data.pop("logo"))
    legal_entity.add("topics", "poi")
    if imo:
        legal_entity.add_cast("Company", "imoNumber", imo)

    sanction = h.make_sanction(context, legal_entity)
    sanction.add("reason", company_data.pop("reason"))
    sanction.add("sourceUrl", company_data.pop("documents", None))
    sanction.add("program", program)
    context.emit(legal_entity)
    context.emit(sanction)

    # 'transport/companies' endpoint provides a list of vessel ids associated with legal entities
    related_ships = company_data.pop("ships", None)
    if related_ships:
        for ship_id in related_ships:
            emit_link(context, ship_id, legal_entity.id)

    context.audit_data(
        company_data, ["sanctions", "products", "rel_companies", "tools", "places"]
    )


# def emit_manager_relation(context, vessel, vessel_data, role):
#     data = vessel_data.pop(role)
#     if not data:
#         return
#     manager = context.make("Company")
#     manager.id = context.make_slug("company", data.pop("id"))

#     relation = context.make("UnknownLink")
#     relation.id = context.make_id(vessel.id, manager.id)
#     relation.add("object", vessel.id)
#     relation.add("subject", manager.id)
#     relation.add("role", role)
#     h.apply_date(relation, "startDate", data.pop("date"))
#     context.emit(relation)


# def crawl_management(context: Context, management_data, program):
#     management = context.make("Company")
#     management.id = context.make_id("company", management_data.pop("id"))
#     management.add("name", management_data.pop("name"))
#     management.add("country", management_data.pop("country"))
#     management.add("imoNumber", management_data.pop("imo"))
#     context.emit(management)
#     sanction = h.make_sanction(context, management)
#     sanction.add("program", program)
#     context.emit(sanction)
#     context.audit_data(management_data)


def crawl_vessel(context: Context, vessel_data, program):
    id = vessel_data.pop("id")
    name = vessel_data.pop("name")
    type = vessel_data.pop("type")
    imo_num = vessel_data.pop("imo")
    photo_url = vessel_data.pop("photo")

    vessel = context.make("Vessel")
    vessel.id = context.make_slug("vessel", id)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_num)
    vessel.add("type", type)
    vessel.add("description", vessel_data.pop("info"))
    vessel.add("callSign", vessel_data.pop("callsign"))
    vessel.add("flag", vessel_data.pop("flag"))
    vessel.add("mmsi", vessel_data.pop("mmsi"))
    vessel.add("buildDate", vessel_data.pop("year"))
    vessel.add("grossRegisteredTonnage", vessel_data.pop("weight"))
    vessel.add("deadweightTonnage", vessel_data.pop("dwt"))
    vessel.add("sourceUrl", vessel_data.pop("links"))
    if "no-ship-photo" not in photo_url:
        vessel.add("sourceUrl", photo_url)
    old_data = vessel_data.pop("old_data", [])
    for item in old_data:
        name = item.pop("name")
        flag = item.pop("flag")
        if name:
            vessel.add("previousName", name)
        if flag:  # Only add if flag is not None
            vessel.add("pastFlags", flag)
    vessel.add("topics", "poi")
    if vessel_data.pop("is_shadow"):
        vessel.add("topics", "mare.shadow")

    sanction = h.make_sanction(context, vessel)
    sanction.add("program", program)
    # sanction.add("sourceUrl", link)

    context.emit(vessel)
    context.emit(sanction)

    # for role in ["commerce_manager", "security_manager", "owner"]:
    #     emit_manager_relation(context, vessel, vessel_data, role)

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
            "pi_club",
            "shadow_group",
            "sanctions_regime",
            "categories",
            "sanctions_lifted",
            "s_companies",
            "s_persons",
            "documents",
            "sanctions",
        ],
    )


def crawl(context: Context):
    for link_info in LINKS:
        token = generate_token(WS_CID, WS_API_KEY)
        headers = {"Authorization": token}
        endpoint = link_info["endpoint"]
        data_type = link_info["type"]
        program = link_info["program"]

        url = f"{context.data_url}{endpoint}"
        response = context.fetch_json(url, headers=headers, cache_days=1)
        if not response or response.get("code") != 0:
            context.log.warn("No valid data to parse")
            return
        data = response.get("data")
        for entity_details in data:
            if data_type == "person":
                # pprint(entity_details)
                crawl_person(context, entity_details, program)
            elif data_type == "legal_entity":
                crawl_legal_entity(context, entity_details, program)
            elif data_type == "vessel":
                crawl_vessel(context, entity_details, program)
            # elif data_type == "management":
            #     crawl_management(context, entity_details, program)
