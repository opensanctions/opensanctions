import csv
from typing import Optional, Dict, Any, Generator

from zavod import Context
from zavod import helpers as h

Item = Dict[str, Any]

TYPES = {
    "FOREIGN_ENTITY": "LegalEntity",
    "LEGAL_ENTITY": "LegalEntity",
    "NATURAL_PERSON": "Person",
    "OWNER": "Ownership",
    "CO_OWNER": "Ownership",
}


def company_id(
    context: Context, reg_nr: str, name: Optional[str] = None
) -> Optional[str]:
    if reg_nr:
        return f"oc-companies-lv-{reg_nr}".lower()
    if name is not None:
        return context.make_slug("company", name)
    context.log.warn("No id for company", name=name, reg_nr=reg_nr)
    return None


def person_id(context: Context, row: Item) -> Optional[str]:
    name = h.make_name(
        full=row.get("name"),
        first_name=row.get("forename"),
        last_name=row.get("surname"),
    )
    return context.make_id(
        "person",
        name,
        row["latvian_identity_number_masked"],
        row["birth_date"],
    )


def oc_url(reg_nr: str) -> str:
    return f"https://opencorporates.com/companies/lv/{reg_nr}"


def make_bank_account(context: Context, row: Item):
    account = context.make("BankAccount")
    account.id = context.make_slug("iban", row["sepa"])
    account.add("iban", row["sepa"])
    return account


def parse_register(context: Context, row: Item):
    reg_nr = row["regcode"]
    company = context.make("Company")
    company.id = company_id(context, reg_nr, name=row["name"])
    company.add("name", row["name"])
    company.add("registrationNumber", reg_nr)
    company.add("legalForm", row["type_text"])
    company.add("incorporationDate", row["registered"])
    company.add("address", row["address"])
    company.add("opencorporatesUrl", oc_url(reg_nr))
    company.add("jurisdiction", "lv")
    company.add("dissolutionDate", row["terminated"])
    company.add("status", row["closed"])

    if row["sepa"]:
        bankAccount = make_bank_account(context, row)
        ownership = context.make("Ownership")
        ownership.id = context.make_slug(
            "bankaccountholder", company.id, bankAccount.id
        )
        ownership.add("owner", company)
        ownership.add("asset", bankAccount)
        context.emit(bankAccount)
        context.emit(ownership)

    context.emit(company)


def parse_old_names(context: Context, row: Item):
    company = context.make("Company")
    company.id = company_id(context, row["regcode"])
    company.add("previousName", row["name"])
    if len(company.properties):
        context.emit(company)


def make_officer(context: Context, row: Item):
    officer_type = TYPES.get(row.get("entity_type"), "Person")
    is_person = officer_type == "Person"
    officer = context.make(officer_type)
    h.apply_name(
        officer,
        full=row.get("name"),
        first_name=row.get("forename"),
        last_name=row.get("surname"),
    )
    if is_person:
        officer.id = person_id(context, row)
        officer.add("idNumber", row["latvian_identity_number_masked"])
        officer.add("birthDate", row["birth_date"])
    else:
        officer.id = company_id(
            context, row["legal_entity_registration_number"], row["name"]
        )
    return officer


def parse_officers(context: Context, row: Item):
    rel_type = TYPES.get(row["position"], "Directorship")
    is_ownership = rel_type == "Ownership"
    officer = make_officer(context, row)
    context.emit(officer)

    cid = company_id(context, row["at_legal_entity_registration_number"])
    rel = context.make(rel_type)
    rel.id = context.make_slug(rel_type, officer.id, cid)
    rel.add("role", row["position"])
    rel.add("role", row["governing_body"])
    rel.add("startDate", row["registered_on"])
    if is_ownership:
        rel.add("owner", officer)
        rel.add("asset", cid)
    else:  # Directorship
        rel.add("director", officer)
        rel.add("organization", cid)
    context.emit(rel)


def parse_beneficial_owners(context: Context, row: Item):
    officer = make_officer(context, row)
    officer.add("nationality", row["nationality"])
    officer.add("country", row["residence"])
    cid = company_id(context, row["legal_entity_registration_number"])
    rel = context.make("Ownership")
    rel.id = context.make_slug("OWNER", officer.id, cid)
    rel.add("role", "OWNER")
    rel.add("startDate", row["registered_on"])
    rel.add("owner", officer)
    rel.add("asset", cid)
    context.emit(officer)
    context.emit(rel)


def parse_members(context: Context, row: Item):
    cid = company_id(context, row["at_legal_entity_registration_number"])
    rel = context.make("Ownership")
    rel.add("role", "OWNER")
    rel.add("asset", cid)
    rel.add("sharesCount", row["number_of_shares"])
    rel.add("sharesValue", row["share_nominal_value"])
    rel.add("sharesCurrency", row["share_currency"])
    rel.add("startDate", row["date_from"])
    if row["entity_type"] == "JOINT_OWNERS":
        # owners will be added by `parse_joint_members` based on relation id:
        rel.id = context.make_slug("OWNER", row["id"])
    else:
        officer = make_officer(context, row)
        rel.add("owner", officer)
        rel.id = context.make_slug("OWNER", officer.id, cid)
        context.emit(officer)
    context.emit(rel)


def parse_joint_members(context: Context, row: Item):
    officer = make_officer(context, row)
    rel = context.make("Ownership")
    rel.id = context.make_slug("OWNER", row["member_id"])
    rel.add("owner", officer)
    context.emit(officer)
    context.emit(rel)


def parse_csv(context: Context, path: str) -> Generator[Item, None, None]:
    url = f"https://dati.ur.gov.lv/{path}"
    file_name = path.rsplit("/", 1)[-1]
    data_path = context.fetch_resource(file_name, url)
    with open(data_path) as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            yield row


def crawl(context: Context):
    for row in parse_csv(context, "register/register.csv"):
        parse_register(context, row)
    for row in parse_csv(context, "register/register_name_history.csv"):
        parse_old_names(context, row)
    for row in parse_csv(context, "beneficial_owners/beneficial_owners.csv"):
        parse_beneficial_owners(context, row)
    for row in parse_csv(context, "officers/officers.csv"):
        parse_officers(context, row)
    for row in parse_csv(context, "members/members.csv"):
        parse_members(context, row)
    for row in parse_csv(context, "members/members_joint_owners.csv"):
        parse_joint_members(context, row)
