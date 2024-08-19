from urllib.parse import unquote
import re
from followthemoney.helpers import post_summary

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

ROLES_OF_INTEREST = [
    "Mayor/Executive Mayor",
    "Municipal Manager",
]

REESTABLISHED_MUNICIPALITIES_URL = (
    "https://municipaldata.treasury.gov.za/api/cubes/demarcation_changes/facts"
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


def crawl(context: Context):
    # Fetch the data from the provided URL
    executive_mayors = context.fetch_json(context.data_url)

    # Fetch the reestablished municipalities data from the specific URL
    reestablished_muni = context.fetch_json(REESTABLISHED_MUNICIPALITIES_URL)

    reestablished_municipalities = {}
    if reestablished_muni and "data" in reestablished_muni:
        for fact in reestablished_muni.get("data", []):
            if fact.get("old_code_transition.code") == "disestablished":
                reestablished_municipalities[fact.get("old_demarcation.code")] = (
                    fact.get("new_demarcation.code")
                )

    # Process the relevant persons
    for person_data in executive_mayors.get("data", []):
        if person_data.get("role.role") in ROLES_OF_INTEREST:
            municipality_code = person_data.get("municipality.demarcation_code")

            # Update the municipality code if it was reestablished
            if municipality_code in reestablished_municipalities:
                municipality_code = reestablished_municipalities[municipality_code]

            name = person_data.get("contact_details.name")
            entity = context.make("Person")
            entity.id = context.make_id(name, municipality_code)
            entity.add("name", person_data.get("contact_details.name"))
            entity.add("title", person_data.get("contact_details.title"))
            entity.add(
                "email", clean_emails(person_data.get("contact_details.email_address"))
            )
            entity.add(
                "phone", clean_phones(person_data.get("contact_details.phone_number"))
            )
            # entity.add("topics", "role.pep")

            role = person_data.get("role.role")
            position_label = f"{role} of the {municipality_code}"

            position = h.make_position(
                context,
                position_label,
                country="za",
                topics=["gov.muni"],
            )
            # position_property = post_summary(
            #     org_name,
            #     role,
            # )
            # entity.add("position", position_property)

            categorisation = categorise(context, position, True)

            if categorisation.is_pep:
                occupancy = h.make_occupancy(
                    context,
                    entity,
                    position,
                    # no_end_implies_current=False,
                )

            if occupancy:
                context.emit(entity, target=True)
                context.emit(position)
                context.emit(occupancy)
