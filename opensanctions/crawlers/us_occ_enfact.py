import json
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    return h.parse_date(date, ["%m/%d/%Y"])


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.source.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for record in data:
        bank = context.make("Company")
        charter_no = record.pop("CharterNumber")
        bank_name = record.pop("BankName")
        bank.id = context.make_slug(charter_no, bank_name)
        bank.add("name", bank_name)
        bank.add("registrationNumber", charter_no)
        bank.add("country", "us")
        bank.add("topics", "fin.bank")
        if bank.id is not None:
            context.emit(bank)
        company_name = record.pop("CompanyName")
        first_name = record.pop("FirstName")
        last_name = record.pop("LastName")
        if company_name:
            entity = context.make("Company")
            entity.id = context.make_id(charter_no, bank_name, company_name)
            entity.add("name", company_name)
        else:
            entity = context.make("Person")
            entity.id = context.make_id(charter_no, bank_name, first_name, last_name)
            h.apply_name(entity, first_name=first_name, last_name=last_name)
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

        sanction = h.make_sanction(context, entity)
        sanction.add("startDate", record.pop("CompleteDate", None))
        sanction.add("endDate", record.pop("TerminationDate", None))
        sanction.add("program", record.pop("EnforcementTypeDescription", None))
        sanction.add("authorityId", record.pop("DocketNumber", None))
        # context.inspect(record)
        context.emit(entity, target=True)
        context.emit(sanction)
