from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.gpt import run_image_prompt

prompt = """
  Extract structured data from the following page of a PDF document. Return 
  a JSON list (`providers`) in which each object represents an medical provider.
  Each object should have the following fields: `excluded_provider`, `npi`,
  `associated_legal_entity`, `persons_controlling_interest`, `contract_termination_date`,
  `nevada_medicaid_sanction_period_end_date`.
  Exclude keys which have no value.
"""


def crawl_item(row: Dict[str, str], context: Context):
    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("excluded_provider"), row.get("npi", None))
    entity.add("name", row.pop("excluded_provider"))
    entity.add("npiCode", row.pop("npi", None))
    entity.add("topics", "debarment")
    entity.add("country", "us")

    if associated_entity_name := row.pop("associated_legal_entity", None):
        associated_entity = context.make("LegalEntity")
        associated_entity.id = context.make_id(associated_entity_name)
        associated_entity.add("name", associated_entity_name)

        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, "related", associated_entity.id)
        link.add("object", entity)
        link.add("subject", associated_entity)

        context.emit(link)
        context.emit(associated_entity)

    if controlling_interest_name := row.pop("persons_controlling_interest", None):
        person = context.make("Person")

        person.id = context.make_id(controlling_interest_name)
        person.add("name", controlling_interest_name)

        link = context.make("Ownership")
        link.id = context.make_id(entity.id, "owner", person.id)
        link.add("asset", entity)
        link.add("owner", person)

        context.emit(link)
        context.emit(person)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("contract_termination_date"))
    h.apply_date(
        sanction, "endDate", row.pop("nevada_medicaid_sanction_period_end_date", None)
    )

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "oig_exclusion_date",
            "oig_reinstate_date",
            "provider_type",
            "sanction_tier",
        ],
    )


def crawl_pdf_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='NV Exclusion List ']")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path, max_tokens=4096)
        assert "providers" in data, data
        for item in data.get("providers", []):
            print(item)
            crawl_item(item, context)
