from typing import Dict

from zavod import Context, helpers as h
from rigour.mime.types import PDF


from zavod.shed.gpt import run_image_prompt


prompt = """
  Extract structured data from the following page of a PDF document. Return 
   a JSON list (`providers`) in which each object represents a medical provider.
   Each object should have the following fields: `last_name`, `first_name`,
   `npi`, `effective_date`, `reason`.
   Return an empty string for unset fields.
 """


def crawl_item(row: Dict[str, str], context: Context):
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    npi = row.pop("npi")

    if first_name:
        entity = context.make("Person")
        entity.id = context.make_id(last_name, first_name, npi)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, npi)
        entity.add("name", last_name)

    entity.add("npiCode", npi)
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("reason", row.pop("reason"))

    context.emit(entity)
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
