from zavod import Context
from zavod import helpers as h
import re
from typing import Dict


def clean_address(text):
    if not text:
        return None

    # regex to match entries wih pattern from a) to z)
    patterns = [
        r"\b[a-z]\)\s(.*?)(?=\s[a-z]\)|$)",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL | re.VERBOSE)
        if matches:
            return [match.strip(", ") for match in matches]
    else:
        return text.split("\n")


def extract_passport_no(text):
    if not text:
        return None
    pattern = r"\b[A-Z0-9]{5,}\b"
    matches = re.findall(pattern, text)

    return matches


def crawl_person(context: Context, record):
    person_details = record.get("mesureDetails")
    first_name = person_details.pop("prenom")
    last_name = record.get("nom")
    gender = person_details.pop("sexe")
    dob = person_details.pop("dateNaissance")
    if dob:
        dob = h.parse_date(dob, ["%d/%m/%Y"])
    place_of_birth = person_details.pop("lieuNaissance")
    nationality = person_details.pop("nationalite")
    title = person_details.pop("titre")
    passport = person_details.pop("passeport")

    person = crawl_common(context, record, "Person")
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("gender", gender)
    person.add("birthDate", dob)
    person.add("birthPlace", place_of_birth)
    person.add("nationality", re.split(r",\s*|/|;", nationality))
    person.add("title", title)
    person.add("passportNumber", extract_passport_no(passport))

    context.emit(person, target=True)
    context.audit_data(person_details, ignore=["autoriteMesure"])


def crawl_common(context: Context, data: Dict[str, str], schema: str):
    entity_details = data.get("mesureDetails")

    alias = entity_details.pop("alias")
    address = entity_details.pop("adresse")
    notes = entity_details.pop("autresInfos")
    reason = entity_details.pop("motifs")
    legal_basis = entity_details.pop("fondementJuridique")
    sanction_regime = entity_details.pop("regimeSanction")
    expiration_date = entity_details.pop("dateExpiration")
    if expiration_date:
        expiration_date = h.parse_date(expiration_date, ["%d/%m/%Y"])

    entity = context.make(schema)
    entity.id = context.make_id("mc-freezes", data.get("mesureId"))
    entity.add("alias", re.split(r"[;”]", alias))
    entity.add("address", clean_address(address))
    entity.add("notes", notes)
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", reason)
    sanction.add("provisions", legal_basis)
    sanction.add("program", sanction_regime)
    sanction.add("endDate", expiration_date)

    context.emit(sanction)
    return entity


def crawl_company(context: Context, record):
    org = crawl_common(context, record, "Organization")
    org.add("name", record.get("nom"))

    context.emit(org, target=True)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)
    count_person = 0
    count_company = 0
    for record in data:
        record_type = record.get("nature")
        if record_type == "Personne physique":
            crawl_person(context, record)
            count_person += 1
        elif record_type in ["Personne morale", "Navire"]:
            crawl_company(context, record)
            count_company += 1
        else:
            context.log.warn(f"Entity type not handled: {record_type}")
