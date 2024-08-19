from urllib.parse import unquote
import re
from followthemoney.helpers import post_summary

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

roles_of_interest = [
    "Deputy Mayor/Executive Mayor",
    "Mayor/Executive Mayor",
    "Municipal Manager",
]


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
    data = context.fetch_json(context.data_url)

    # Process the relevant persons
    for person_data in data.get("data", []):
        if person_data.get("role.role") in roles_of_interest:
            municipality = (person_data.get("municipality.demarcation_code"),)
            name = (person_data.get("contact_details.name"),)
            entity = context.make("Person")
            entity.id = context.make_id(municipality, name)
            entity.add("name", person_data.get("contact_details.name"))
            entity.add("title", person_data.get("contact_details.title"))
            # entity.add("role", "Deputy Mayor/Executive Mayor")
            entity.add(
                "email", clean_emails(person_data.get("contact_details.email_address"))
            )
            entity.add(
                "phone", clean_phones(person_data.get("contact_details.phone_number"))
            )
            # entity.add("topics", "role.pep")

            role = person_data.get("role.role")
            muni_name = person_data.get("municipality.demarcation_code")
            position_label = f"{role} of the {muni_name}"

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
                print(f"PEP: {position_label}")
                context.emit(entity, target=True)
                context.emit(position)
                context.emit(occupancy)
