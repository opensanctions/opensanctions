from urllib.parse import unquote
import re

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

DEMARCATION_CHANGES_URL = (
    "https://municipaldata.treasury.gov.za/api/cubes/demarcation_changes/facts"
)
MUNICIPALITIES_URL = (
    "https://municipaldata.treasury.gov.za/api/cubes/municipalities/facts"
)
REGEX_NAME_CLEAN = re.compile(
    r"""
    \((
        Acting\sCity\sManager|
        Acting\sCfo|
        Assistant\sDirector|
        Deputy\sCFO|
        Acting|
        CFO|
        DA|
        ANC|
        PDC|
    )\)|
    \(Municipal\sManage|
    Acting\sMunicipal\sManager|
    """,
    re.VERBOSE + re.IGNORECASE,
)


def clean_emails(emails):
    out = []
    for email in h.multi_split(emails, ["/", ",", " or "]):
        if email is None:
            continue
        email = unquote(email).strip().rstrip(".")
        out.append(email)
    return out


def clean_phones(phones):
    out = []
    for phone in h.multi_split(
        phones, [",", "/", "(1)", "(2)", "(3)", "(4)", "(5)", "(6)", "(7)", "(8)"]
    ):
        phone = re.sub(
            r"(ex|ext|extension|fax|tel|\:|\-)", "", phone, flags=re.IGNORECASE
        )
        out.append(phone.strip())
    return out


def clean_name(name):
    return REGEX_NAME_CLEAN.sub("", name).strip()


def crawl(context: Context):
    officials = context.fetch_json(context.data_url, cache_days=1).get("data")

    # All munis
    municipalities = dict()
    for muni in context.fetch_json(MUNICIPALITIES_URL, cache_days=1).get("data"):
        municipalities[muni.get("municipality.demarcation_code")] = muni

    # Disestablished muni codes
    demarcation_changes = context.fetch_json(
        DEMARCATION_CHANGES_URL,
        cache_days=1,
    ).get("data")
    disestablished_municipalities = set()
    for fact in demarcation_changes:
        if fact.get("old_code_transition.code") == "disestablished":
            # Add the old demarcation code to the set
            disestablished_municipalities.add(fact.get("old_demarcation.code"))

    # Process the relevant persons
    for person_data in officials:
        municipality_code = person_data.get("municipality.demarcation_code")

        # Skip the municipality code if it was disestablished
        if municipality_code in disestablished_municipalities:
            continue

        name = clean_name(person_data.get("contact_details.name"))
        name = context.lookup_value("normalize_name", name, name)
        if h.is_empty(name):
            continue
        if "vacant" in name.lower():
            context.log.warning("Double-check vacant position", name=name)

        entity = context.make("Person")
        entity.id = context.make_id(name, municipality_code)
        entity.add("name", name)
        entity.add("title", person_data.get("contact_details.title"))
        entity.add(
            "email", clean_emails(person_data.get("contact_details.email_address"))
        )
        entity.add(
            "phone", clean_phones(person_data.get("contact_details.phone_number"))
        )

        role = person_data.get("role.role")

        is_pep = None
        role_res = context.lookup("roles", role)
        if role_res:
            is_pep = role_res.is_pep
        else:
            context.log.warning("Uncategorised role", role=role)

        position = h.make_position(
            context,
            f"{role} of {municipalities[municipality_code].get('municipality.long_name')}",
            country="za",
            topics=["gov.muni"],
            subnational_area=municipality_code,
        )

        categorisation = categorise(context, position, is_pep)

        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context,
            entity,
            position,
        )

        if occupancy:
            context.emit(entity)
            context.emit(position)
            context.emit(occupancy)
