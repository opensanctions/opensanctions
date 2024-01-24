from zavod import Context
from typing import Any, Dict


HEADERS = {"Cookie": "ORIGIN=KPK1&KPK2.KPK1"}


def fetch_data(context: Context, offset, limit):
    page_url = f"https://erar.si/api/omejitve/?&draw=2&columns%5B0%5D%5Bdata%5D=org_naziv&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=omejitev_do&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=subjekt_naziv&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=od&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=do&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=st_transakcij&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&start={offset}&length={limit}&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1706090961920"

    response = context.fetch_response(page_url, headers=HEADERS)
    return response.json()


def create_entities(context: Context, record: Dict[str, Any]):
    organization = context.make("Organization")

    organization_name = record.get("org_naziv")
    organization_number = record.get("org_sifrapu")

    organization.id = context.make_id(organization_number or organization_name)
    organization.add("name", organization_name)
    organization.add("idNumber", organization_number)

    legal_entity = context.make("LegalEntity")
    subject_name = record.get("subjekt_naziv")
    registration_number = record.get("subjekt_maticna")

    if not (subject_name and registration_number):
        context.log.error("Subject name and registration number not found")
        return

    legal_entity.id = context.make_id(registration_number or subject_name)
    legal_entity.add("name", subject_name)
    legal_entity.add("registrationNumber", registration_number)
    legal_entity.add("taxNumber", record.get("subjekt_davcna"))

    if registration_number:
        legal_entity.add(
            "sourceUrl",
            f"https://erar.si/transakcija/placnik/{organization_number}/prejemnik/{registration_number}",
        )

    validity_link = context.make("UnknownLink")
    validity_link.id = context.make_id(legal_entity.id)
    validity_link.add("startDate", record.get("od"))

    if not record.get("do").startswith("9999"):
        validity_link.add("endDate", record.get("do"))

    validity_link.add(
        "description",
        "When the restriction is valid from, and when it ends. No end date implies it is valid until canceled",
    )
    validity_link.add("subject", legal_entity)
    validity_link.add("object", organization)

    context.emit(legal_entity, target=True)
    context.emit(organization)
    context.emit(validity_link)
    context.audit_data(record, ignore=["omejitev_do", "st_transakcij"])


def parse_data(context: Context, response: Dict[str, Any]):
    data = response.get("data")

    for record in data:
        create_entities(context, record)


def crawl(context: Context):
    offset = 0
    limit = 100
    records_total = limit
    while True:
        response = fetch_data(context, offset, limit)
        records_total = (
            response.get("recordsTotal") if records_total == limit else records_total
        )
        parse_data(context, response)

        if offset + limit >= records_total:
            break

        offset += limit
        context.log.info(f"Processed entities from page {offset//limit}")
