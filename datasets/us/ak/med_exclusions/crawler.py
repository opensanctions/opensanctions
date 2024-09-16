from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h

from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`providers`) in which each object represents an medical provider.
Each object should have the following fields: `exclusion_date`, `last_name`,
`first_name`, `provider_type`, `exclusion_authority`, `exclusion_reason`.
Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("last_name"):
        return

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

    entity.add("sector", row.pop("provider_type"))

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("exclusion_reason"))
    sanction.add("authority", row.pop("exclusion_authority"))
    h.apply_date(sanction, "startDate", row.pop("exclusion_date"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path)[1:]:
        data = run_image_prompt(context, prompt, page_path)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
