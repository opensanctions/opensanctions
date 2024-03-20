from zavod import Context
from typing import Any, Dict


def create_entities(context: Context, record: Dict[str, Any]):
    legal_entity = context.make("LegalEntity")
    subject_name = record.pop("subjekt_naziv")
    registration_number = record.pop("subjekt_maticna")

    if subject_name == "" and registration_number is None:
        context.log.info(
            "Subject name and registration number not found", record=record
        )
        return

    legal_entity.id = context.make_id(registration_number or subject_name)
    legal_entity.add("name", subject_name)
    legal_entity.add("registrationNumber", registration_number)
    legal_entity.add("taxNumber", record.pop("subjekt_davcna"))
    legal_entity.add("topics", "debarment")
    legal_entity.add("country", "si")

    organization_name = record.pop("org_naziv")
    organization_number = record.pop("org_sifrapu")

    if registration_number:
        legal_entity.add(
            "sourceUrl",
            f"https://erar.si/transakcija/placnik/{organization_number}/prejemnik/{registration_number}",
        )

    start_date = record.pop("od")
    end_date = record.pop("do")
    if end_date.startswith("9999"):
        end_date = "cancellation"
    else:
        end_date = f"to {end_date}"

    legal_entity.add(
        "program",
        f"{organization_name} is restricted from procurement from {subject_name} from {start_date} until {end_date} due to ownership or management role of a public official in {organization_name} or their family member. This is a preventative restriction by Komisija za preprečevanje korupcije and implies no wrongdoing.",
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
