from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.gpt import run_image_prompt

prompt = """
  Extract structured data from the following page of a PDF document. Return 
  a JSON list (`providers`) in which each object represents an medical provider.
  Each object should have the following fields: `name`, `npi`,
  `medical_provider`, `provider_type`, `associated_legal_entity`,
  `persons_controlling_interest`, `termination_date`, `sanction_tier`, `period`, `end_date`,
  `oig_exclusion_date`, `oig_reinstate_date`.
  Return an empty string for unset fields.
"""


def crawl_item(row: Dict[str, str], context: Context):

    # We already crawl the federal dataset on another crawler
    if row.pop("sanction_tier") == "Federal":
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("name"), row.get("npi"))
    entity.add("name", row.pop("name"))
    entity.add("npiCode", row.pop("npi"))
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    entity.add("idNumber", row.pop("license_num"))
    entity.add("country", "us")

    if associated_entity_name := row.pop("associated_legal_entity"):
        associated_entity = context.make("LegalEntity")
        associated_entity.id = context.make_id(associated_entity_name)
        associated_entity.add("name", associated_entity_name)

        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, associated_entity.id)
        link.add("object", entity)
        link.add("subject", associated_entity)

        context.emit(link)

    if controlling_interest_name := row.pop("persons_controlling_interest"):
        person = context.make("Person")

        person.id = context.make_id(controlling_interest_name)
        person.add("name", controlling_interest_name)

        link = context.make("Ownership")
        link.id = context.make_id(entity.id, associated_entity.id)
        link.add("asset", entity)
        link.add("owner", person)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_date"))
    if row.pop("sanction_period") != "Permanent":
        h.apply_date(sanction, "endDate", row.pop("end_date"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["oig_exclusion_date", "oig_reinstate_date"])


def crawl_pdf_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='NV Exclusion List ']")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for page_path in h.make_pdf_page_images(path)[1:]:
        data = run_image_prompt(context, prompt, page_path)
        assert "providers" in data, data
        for item in data.get("providers", []):
            crawl_item(item, context)
