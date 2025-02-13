import json
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h

IGNORE = [
    "TerminationDate",
    "Amount",
    "EnforcementInstIAPType",
    "HasPdf",
    "HasTerminationPdf",
    "TerminationDocumentNumber",
    "TerminationDocuments",
]


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for record in data:
        # orig_record = dict(record)
        charter_no = record.pop("CharterNumber")
        doc_numbers = record.pop("StartDocuments", None)
        docket_number = record.pop("DocketNumber", None)
        bank_name = record.pop("Institution")
        bank = context.make("Company")
        bank.id = context.make_slug(charter_no, bank_name)
        if bank.id is not None:
            bank.add("name", bank_name)
            bank.add("registrationNumber", charter_no)
            bank.add("country", "us")
            bank.add("topics", "fin.bank")
            context.emit(bank)
        company_name = record.pop("Company")
        person_name = record.pop("Individual")
        entity = context.make("Company")
        if company_name and company_name != person_name:
            entity.id = context.make_id(charter_no, bank_name, company_name)
            entity.add("name", company_name)
        elif person_name:
            # Roughly make first and last name for ID consistent with original IDs
            parts = person_name.split(" ")
            forenames, lastname = parts[:-1], parts[-1]
            entity = context.make("Person")
            entity.id = context.make_id(
                charter_no, bank_name, " ".join(forenames), lastname
            )

            h.apply_name(entity, full=person_name)
        if entity.id is None:
            entity.id = bank.id
        if entity.id is None:
            context.log.error("Entity has no ID", record=record)
            continue

        entity.add("country", "us")
        entity.add("topics", "crime.fin")

        location = record.pop("Location")
        if location:
            addr = h.make_address(context, full=location)
            h.apply_address(context, entity, addr)

        sanction = h.make_sanction(
            context, entity, key=(docket_number, sorted(doc_numbers))
        )
        h.apply_date(sanction, "startDate", record.pop("StartDate"))
        # sanction.add("endDate", record.pop("TerminationDate", None))
        sanction.add("program", record.pop("TypeDescription"))
        sanction.add("authorityId", docket_number)
        sanction.add("provisions", record.pop("TypeCode"))
        sanction.add("reason", record.pop("SubjectMatters"))

        context.audit_data(record, ignore=IGNORE)
        context.emit(entity)
        context.emit(sanction)
