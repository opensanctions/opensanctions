import string
import random
import hashlib
import base64

from datetime import datetime, timezone
from os import environ as env
from typing import Optional

from zavod import Context, helpers as h

# Make sure variables don't get interpolated by the shell.
WS_CID = env.get("OPENSANCTIONS_WS_CID")
WS_API_KEY = env.get("OPENSANCTIONS_WS_API_KEY")
WS_API_DOCS = env.get("OPENSANCTIONS_WS_API_DOCS")

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
    {  # companies associated with ships
        "endpoint": "transport/companies",
        "type": "legal_entity",
        "program": "Companies associated with the ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {  # persons associated with ships
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
    signature = hashlib.sha256(signature_input.encode()).hexdigest().lower()
    # 4. Build raw token string = signature + cid + sid + timestamp
    raw_token = f"{signature}{cid}{sid}{timestamp}"
    # 5. Base64 encode
    token = base64.b64encode(raw_token.encode()).decode()
    return token


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
    rel_schema: str = "UnknownLink",
    from_prop: str = "subject",
    to_prop: str = "object",
):
    if not party_info:
        return
    company_id_raw = party_info.pop("id")
    start_date = party_info.pop("date")
    care_of_id_raw = party_info.pop("co_id", None)

    if rel_role == "owner":
        rel_schema = "Ownership"
        from_prop = "owner"
        to_prop = "asset"

    emit_relation(
        context,
        context.make_slug("company", company_id_raw),
        vessel_id_slug,
        rel_schema,
        rel_role,
        from_prop,
        to_prop,
        start_date,
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
            context.make_slug("company", company_id_raw),
            context.make_slug("company", care_of_id_raw),
            rel_schema,
            rel_role="c/o",
            from_prop=from_prop,
            to_prop=to_prop,
        )

    context.audit_data(party_info)


def emit_relation(
    context: Context,
    subject_id,
    object_id,
    rel_schema: str = "UnknownLink",
    rel_role: Optional[str] = None,
    from_prop: str = "subject",
    to_prop: str = "object",
    start_date: Optional[str] = None,
):
    relation = context.make(rel_schema)
    relation.id = context.make_id(object_id, rel_role, subject_id)
    relation.add(from_prop, subject_id)
    relation.add(to_prop, object_id)
    relation.add("role", rel_role)
    h.apply_date(relation, "startDate", start_date)
    context.emit(relation)


def crawl_person(context: Context, person_data, program, endpoint):
    id = person_data.pop("id")
    name_en = person_data.pop("name_en")
    name_uk = person_data.pop("name_uk")
    name_ru = person_data.pop("name_ru")
    positions = person_data.pop("positions", None)
    position = person_data.pop("position", None)
    birth_date = person_data.pop("date_bd")
    death_date = person_data.pop("date_death", None)
    if "- " in birth_date:
        birth_date, death_date = split_dob_dod(birth_date)
    pob = person_data.pop("city_bd", None)
    links = person_data.pop("links", None)

    person = context.make("Person")
    person.id = context.make_slug("person", id)
    person.add("name", name_en, lang="eng")
    person.add("name", name_uk, lang="ukr")
    person.add("name", name_ru, lang="rus")
    person.add("citizenship", person_data.pop("citizenships", None))
    person.add("taxNumber", person_data.pop("itn"))
    person.add("position", positions)
    person.add("position", position)
    person.add("position", person_data.pop("positions_main", None))
    person.add("position", person_data.pop("positions_other", None))
    h.apply_date(person, "birthDate", birth_date)
    h.apply_date(person, "deathDate", death_date)
    person.add("topics", "poi")
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
        for ship_id_raw in related_ships:
            role = "captain" if endpoint == "transport/captains" else None
            emit_relation(
                context,
                person.id,
                context.make_slug("vessel", ship_id_raw),
                rel_role=role,
            )

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
    legal_entity.add("sourceUrl", company_data.pop("logo"))
    legal_entity.add("topics", "poi")
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
                context, legal_entity.id, context.make_slug("vessel", ship_id_raw)
            )

    context.audit_data(
        company_data, ["sanctions", "products", "rel_companies", "tools", "places"]
    )


def crawl_manager(context: Context, management_data, program):
    manager = context.make("Company")
    manager.id = context.make_slug("company", management_data.pop("id"))
    manager.add("name", management_data.pop("name"))
    # We null falsy names via the lookups (and we end up with some loose ends because of that)
    # Linked companies may not exist:
    # 'ua-ws-company-22',Company,'Company'
    # 'ua-ws-company-32',Company,'Company'
    # 'ua-ws-company-238',Company,'Company'
    if not manager.get("name"):
        return
    manager.add("country", management_data.pop("country"))
    manager.add("imoNumber", management_data.pop("imo"))
    context.emit(manager)
    sanction = h.make_sanction(context, manager)
    sanction.add("program", program)
    context.emit(sanction)
    context.audit_data(management_data)


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
    if "no-ship-photo" not in photo_url:
        vessel.add("sourceUrl", photo_url)
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

    for role in ["commerce_manager", "security_manager", "owner"]:
        party_info = vessel_data.pop(role, None)
        crawl_ship_relation(context, party_info, vessel.id, role)

    pi_club_info = vessel_data.pop("pi_club", None)
    if pi_club_info:
        for club in pi_club_info:
            pi_club = context.make("Organization")
            pi_club.id = context.make_slug("organization", club.pop("id"))
            pi_club.add("name", club.pop("name"))
            context.emit(pi_club)
            emit_relation(context, pi_club.id, vessel.id, rel_role="P&I Club")

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
        ],
    )


def check_updates(context: Context):
    doc = context.fetch_html(WS_API_DOCS)
    # Have any new sections been added?
    change_log = doc.xpath(".//main[@class='relative']")
    assert len(change_log) == 1, change_log
    h.assert_dom_hash(change_log[0], "18328f23fa487338c88a408674d1c4682a1f32c6")
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
                crawl_person(context, entity_details, program, endpoint)
            elif data_type == "legal_entity":
                crawl_legal_entity(context, entity_details, program)
            elif data_type == "vessel":
                crawl_vessel(context, entity_details, program)
            elif data_type == "management":
                crawl_manager(context, entity_details, program)
            else:
                context.log.warn(f"Unknown data type: {data_type}")
