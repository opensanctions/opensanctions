from datetime import datetime
from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`providers`) in which each object represents an medical provider.
Distinct record rows alternate between grey and white backgrounds. The name
of a provider might be split onto the next row within a record.
Each object should have the following fields: `provider_name`, `title`,
`npi_number`, `street`, `city`, `state`, `zip`, `action`, `effective_date`,
`expiration_date`.
Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(
        context,
        street=row.pop("street"),
        city=row.pop("city"),
        state=row.pop("state"),
        country_code="US",
        postal_code=row.pop("zip"),
    )

    if not row.get("title"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("npi_number"), row.get("provider_name"))
        entity.add("name", row.pop("provider_name"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("npi_number"), row.get("provider_name"))
        h.apply_name(entity, full=row.pop("provider_name"))
        entity.add("title", row.pop("title"))

    entity.add("npiCode", row.pop("npi_number"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("provisions", row.pop("action"))

    ended = False

    if row.get("expiration_date") and row.get("expiration_date").upper() not in [
        "PERMANENT",
        "DECEASED",
    ]:
        h.apply_date(sanction, "endDate", row.pop("expiration_date"))
        end_date = sanction.get("endDate")
        ended = end_date != [] and end_date[0] < context.data_time_iso
    else:
        row.pop("expiration_date")

    if not ended:
        entity.add("topics", "debarment")

    context.emit(entity, target=not ended)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path)[1:]:
        data = run_image_prompt(context, prompt, page_path, max_tokens=4096)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
