from typing import Dict

from zavod import Context, helpers as h
from rigour.mime.types import PDF


from zavod.shed.gpt import run_image_prompt


prompt = """
 Extract structured data from the following page of a PDF document. Return 
  a JSON list (`providers`) in which each object represents an medical provider.
  Each object should have the following fields: `last_name`, `first_name`,
  `npi`, `address_1`, `address_2`, `city`, `state`, `zip`, `action_date`,
  `excluded_terminated`, `reason`.
  Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):
    address = h.make_address(
        context,
        street=row.pop("address_1"),
        street2=row.pop("address_2"),
        postal_code=row.pop("zip"),
        city=row.pop("city"),
        state=row.pop("state"),
        country_code="us",
    )
    npi = row.pop("npi")
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")

    if first_name:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, first_name, npi)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, npi)
        entity.add("name", last_name)

    entity.add("npiCode", h.multi_split(npi, ","))
    entity.add("topics", "debarment")
    entity.add("country", "us")
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("action_date"))
    sanction.add("reason", row.pop("reason"))
    sanction.add("provisions", row.pop("excluded_terminated"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl_pdf_url(context: Context):
    doc = context.fetch_text(context.data_url)
    # The link is inside a JSON in a script
    info_txt = doc[doc.find("FileLeafRef") :]
    return (
        "https://www.wvmmis.com/WV%20Medicaid%20Provider%20SanctionedExclusion/"
        + info_txt[15 : info_txt.find(",") - 1]
    )


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "providers" in data, data
        for item in data.get("providers"):
            crawl_item(item, context)
