import re
from typing import Any, Dict

from zavod import Context
from zavod import helpers as h

ALIAS_SPLITS = [
    "; a)",
    "; b)",
    "; c)",
    "; d)",
    "; e)",
    "; f)",
    "; g)",
    "; h)",
    "; i)",
    "; j)",
    "; k)",
    "; l)",
    "; m)",
    "; n)",
    "; o)",
    "; p)",
    "  a) ",
    "  b) ",
    "  c) ",
    "  d) ",
    ";;",
    ",;",
    ";",
    " ou ",
    "Egalement connue sous le nom:",
    "Egalement connue sous les noms:",
    "Autrement connu sous le nom de:",
    "Autrement connue sous le nom de:",
    "Anciennement connue sous les noms:",
    "Nom de scène:",
    "(autre dénomination :",
    "(autres dénominations :",
    "autres dénominations:",
]


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
    aliases = details.pop("alias")
    aliasProp = "alias"
    # TODO: #2656
    # Aliases splitting is a good candidate for LLM-backed name splitting helper
    # https://github.com/opensanctions/opensanctions/issues/2656
    for alias in h.multi_split(aliases, ALIAS_SPLITS):
        alias = alias.strip()
        if not alias:
            continue
        if "Pseudonymes peu fiables" in alias or "Pseudonyme peu fiable" in alias:
            # After we see a "Pseudonymes peu fiables :" (non-reliable pseudonyms) we switch to the "weakAlias" property
            aliasProp = "weakAlias"
            continue

        if "/" in alias:
            result = context.lookup("aliases", alias)
            if not result:
                context.log.warn(f"Alias not found in the lookups: {alias.strip()}")
                entity.add(aliasProp, alias)
            else:
                for a in result.aliases:
                    entity.add(aliasProp, a)
        else:
            entity.add(aliasProp, alias.strip())
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
        name = data.pop("nom")
        first_name = details.pop("prenom")
        # Entities that don't have a first name usually have their full name in the "Nom" field.
        if not first_name:
            h.apply_name(entity, full=name)
        else:
            h.apply_name(entity, first_name=first_name, last_name=name)
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
    context.emit(entity)
    context.audit_data(details, ignore=["autoriteMesure", "dateNaissance"])
    context.audit_data(data)


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=1)
    for record in data:
        crawl_entity(context, record)
