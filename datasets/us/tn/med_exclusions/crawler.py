from typing import Dict

from zavod import Context, helpers as h
from rigour.mime.types import PDF


from zavod.shed.gpt import run_image_prompt


prompt = """
  Extract structured data from the following page of a PDF document. Return 
   a JSON list (`providers`) in which each object represents an medical provider.
   Each object should have the following fields: `last_name`, `first_name`,
   `npi`, `effective_date`, `reason`.
   Return an empty string for unset fields.
 """


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("first_name"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name"), row.get("npi"))
        entity.add("name", row.pop("last_name"))

    else:
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("last_name"), row.get("first_name"), row.get("npi")
        )
        h.apply_name(
            entity, first_name=row.pop("first_name"), last_name=row.pop("last_name")
        )

    if npi := row.pop("npi"):
        entity.add("npiCode", npi)
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("reason", row.pop("reason"))

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
