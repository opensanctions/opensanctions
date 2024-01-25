from zavod import Context
from typing import Any, Dict


def create_entities(context: Context, record: Dict[str, Any]):
    organization_name = record.pop("org_naziv")
    organization_number = record.pop("org_sifrapu")

    legal_entity = context.make("LegalEntity")
    subject_name = record.pop("subjekt_naziv")
    registration_number = record.pop("subjekt_maticna")

    if not (subject_name and registration_number):
        context.log.warning("Subject name and registration number not found")
        return

    legal_entity.id = context.make_id(registration_number or subject_name)
    legal_entity.add("name", subject_name)
    legal_entity.add("registrationNumber", registration_number)
    legal_entity.add("taxNumber", record.pop("subjekt_davcna"))
    legal_entity.add("topics", "debarment")

    if registration_number:
        legal_entity.add(
            "sourceUrl",
            f"https://erar.si/transakcija/placnik/{organization_number}/prejemnik/{registration_number}",
        )

    start_date = record.pop("od")
    end_date = record.pop("do")
    if end_date.startswith("9999"):
        end_date = "until cancelation"
    else:
        end_date = f"to {end_date}"

    legal_entity.add(
        "program",
        f"""{subject_name} is subject to business restriction, valid from {start_date} and applies {end_date}. 
         Following the regulations, the entity is prohibited because a public sector, {organization_name}, must avoid awarding contracts 
         or special rights to entities where an official or their family has significant involvement or ownership exceeding 5% """,
    )

    context.emit(legal_entity, target=True)
    context.audit_data(record, ignore=["omejitev_do", "st_transakcij"])


def parse_data(context: Context, response: Dict[str, Any]):
    data = response.get("data")

    for record in data:
        create_entities(context, record)


def crawl(context: Context):
    response = context.fetch_json(context.data_url, cache_days=1)
    parse_data(context, response)
