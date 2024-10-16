from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h

from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`providers`) in which each object represents an medical provider.
The name of a provider might be split onto the next row within a record.
Each object should have the following fields: `last_name`,
`first_name`, `middle_initial`, `medicaid_provider_id`, `provider_type`,
`exclusion_date`, `reinstatement_date`.
Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):

    if first_name := row.pop("first_name"):
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("last_name"), first_name, row.get("exclusion_date")
        )
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=row.pop("last_name"),
            middle_name=row.pop("middle_initial"),
        )
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name"))
        entity.add("name", row.pop("last_name"))

    entity.add("sector", row.pop("provider_type"))
    entity.add("description", "Provider ID: " + row.pop("medicaid_provider_id"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("exclusion_date"))
    if row.get("reinstatement_date") != "Indefinite":
        h.apply_date(sanction, "endDate", row.pop("reinstatement_date"))
        is_debarred = False
    else:
        row.pop("reinstatement_date")
        is_debarred = True
        entity.add("topics", "debarment")

    context.emit(entity, target=is_debarred)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path, max_tokens=4096)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
