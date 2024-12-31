import re
from typing import Any, Dict

from zavod import Context
from zavod import helpers as h


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


def crawl_entity(context: Context, data: Dict[str, Any]):
    entity_id = data.pop("mesureId")
    status = data.pop("state")
    if status == "withdrawal":
        return
    type_ = data.pop("nature")
    schema = context.lookup_value("schemata", type_)
    if schema is None:
        context.log.error(f"Schema not found for ({entity_id}) {type_}")
        return

    entity = context.make(schema)
    entity.id = context.make_id("mc-freezes", entity_id)
    entity.add("topics", "sanction")
    details = data.pop("mesureDetails")
    alias = details.pop("alias")
    entity.add("alias", re.split(r"[;”]", alias))
    address = details.pop("adresse")
    entity.add("address", clean_address(address))
    entity.add("notes", details.pop("autresInfos"))

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", details.pop("motifs"))
    sanction.add("provisions", details.pop("fondementJuridique"))
    sanction.add("program", details.pop("regimeSanction"))
    exp_date = details.pop("dateExpiration")
    h.apply_date(sanction, "endDate", exp_date)

    if schema == "Person":
        h.apply_name(
            entity,
            first_name=details.pop("prenom"),
            last_name=data.pop("nom"),
        )
        entity.add("gender", details.pop("sexe"))
        for dob in h.multi_split(details.pop("dateNaissance"), [";"]):
            h.apply_date(entity, "birthDate", dob.strip())
        entity.add("birthPlace", details.pop("lieuNaissance"))
        nationality = details.pop("nationalite")
        entity.add(
            "nationality",
            re.split(r",\s*|/|;", nationality),
            original_value=nationality,
        )
        entity.add("position", details.pop("titre"))
        passport = details.pop("passeport")
        entity.add(
            "passportNumber",
            extract_passport_no(passport),
            original_value=passport,
        )
    else:
        entity.add("name", data.pop("nom"))

    context.emit(sanction)
    context.emit(entity, target=True)
    context.audit_data(details, ignore=["autoriteMesure", "dateNaissance"])
    context.audit_data(data)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)
    for record in data:
        crawl_entity(context, record)
