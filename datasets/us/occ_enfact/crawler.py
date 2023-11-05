import json
from pantomime.types import JSON

from zavod import Context
from zavod import helpers as h

IGNORE = [
    "TerminationDate",
    "Amount",
    "EnforcementInstIAPType",
    "HasPdf",
    "HasTerminationPdf",
    "TerminationDocumentNumber",
]


def parse_date(date):
    return h.parse_date(date, ["%m/%d/%Y"])


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for record in data:
        # orig_record = dict(record)
        charter_no = record.pop("CharterNumber")
        doc_number = record.pop("DocumentNumber", None)
        docket_number = record.pop("DocketNumber", None)
        bank_name = record.pop("BankName")
        bank = context.make("Company")
        bank.id = context.make_slug(charter_no, bank_name)
        if bank.id is not None:
            bank.add("name", bank_name)
            bank.add("registrationNumber", charter_no)
            bank.add("country", "us")
            bank.add("topics", "fin.bank")
            context.emit(bank)
        company_name = record.pop("CompanyName")
        first_name = record.pop("FirstName")
        last_name = record.pop("LastName")
        entity = context.make("Company")
        if company_name:
            entity.id = context.make_id(charter_no, bank_name, company_name)
            entity.add("name", company_name)
        elif first_name or last_name:
            entity = context.make("Person")
            entity.id = context.make_id(charter_no, bank_name, first_name, last_name)
            h.apply_name(entity, first_name=first_name, last_name=last_name)
        if entity.id is None:
            entity.id = bank.id
        if entity.id is None:
            context.log.error("Entity has no ID", record=record)
            continue

        entity.add("country", "us")
        entity.add("topics", "crime.fin")

        addr = h.make_address(
            context,
            city=record.pop("CityName"),
            state=record.pop("StateName"),
            country_code="us",
        )
        record.pop("StateAbbreviation")
        h.apply_address(context, entity, addr)

        sanction = h.make_sanction(context, entity, key=(docket_number, doc_number))
        sanction.add("startDate", record.pop("CompleteDate", None))
        # sanction.add("endDate", record.pop("TerminationDate", None))
        sanction.add("program", record.pop("EnforcementTypeDescription", None))
        sanction.add("authorityId", docket_number)
        sanction.add("provisions", record.pop("EnforcementTypeCode", None))

        context.audit_data(record, ignore=IGNORE)
        context.emit(entity, target=True)
        context.emit(sanction)
