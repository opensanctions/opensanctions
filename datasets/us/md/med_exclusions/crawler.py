from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h

from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`providers`) in which each object represents an medical provider.
Distinct record rows alternate between grey and white backgrounds. The name
of a provider might be split onto the next row within a record.
Each object should have the following fields: `last_name`, `first_name`,
`specialty`, `sanction_type`, `termination_date`, `address`, `city_state_zip`.
Return an empty string for unset fields.
"""

SANCTION_TYPES = {
    "MCFU-A": "Medicaid Fraud Control Unit Adult Abuse",
    "LB": "Licensing Board",
    "HD": "Health Department",
    "HHS": "Department of Health and Human Services Medicaid Medicare Services",
    "F": "Individual or Entity Convicted of Medicaid Fraud",
}


def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(
        context, row.pop("address") + " " + row.pop("city_state_zip")
    )

    if not row.get("first_name"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name"))
        entity.add("name", row.pop("last_name"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("last_name"), row.get("first_name"))
        h.apply_name(
            entity, first_name=row.pop("first_name"), last_name=row.pop("last_name")
        )

    entity.add("sector", row.pop("specialty"))
    entity.add("topics", "debarment")
    entity.add("country", "us")
    entity.add("address", address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_date"))
    sanction.add("description", "Sanction Type: " + row.pop("sanction_type"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
