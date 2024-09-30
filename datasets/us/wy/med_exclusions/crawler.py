from typing import Dict

from zavod import Context, helpers as h
from rigour.mime.types import PDF


from zavod.shed.gpt import run_image_prompt

prompt = """
 Extract structured data from the following page of a PDF document. Return 
 a JSON list (`providers`) in which each object represents an medical provider.
 Each object should have the following fields: `last_name`, `first_name`,
 `business_name`, `provider_type`, `provider_number`, `city`, `state`, `exclusion_date`.
 Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(context, city=row.pop("city"), state=row.pop("state"))
    sector = row.pop("provider_type")
    provider_number = row.pop("provider_number")
    exclusion_date = row.pop("exclusion_date")

    if last_name := row.pop("last_name"):
        person = context.make("Person")
        person.id = context.make_id(row.get("first_name"), last_name, provider_number)
        h.apply_name(person, first_name=row.pop("first_name"), last_name=last_name)
        h.apply_address(context, person, address)
        person.add("sector", sector)
        person.add("description", "Provider Number: " + provider_number)
        person.add("topics", "debarment")
        person.add("country", "us")

        sanction = h.make_sanction(context, person)
        h.apply_date(sanction, "startDate", exclusion_date)

        context.emit(person, target=True)
        context.emit(sanction)

    if business_name := row.pop("business_name"):
        company = context.make("Company")
        company.id = context.make_id(business_name, provider_number)
        company.add("name", business_name)
        h.apply_address(context, company, address)
        company.add("sector", sector)
        company.add("description", "Provider Number: " + provider_number)
        company.add("topics", "debarment")
        company.add("country", "us")

        company_sanction = h.make_sanction(context, person)
        h.apply_date(company_sanction, "startDate", exclusion_date)

        context.emit(company, target=True)
        context.emit(company_sanction)

    if last_name and business_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)

        context.emit(link)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path)[1:]:
        data = run_image_prompt(context, prompt, page_path)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
