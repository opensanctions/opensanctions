from typing import Dict
from rigour.mime.types import PDF
from pdfplumber.page import Page

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    # We already crawl the federal dataset on another crawler
    sanction_tier = row.pop("nevada_medicaid_sanction_tier")
    if sanction_tier.lower() == "federal":
        return

    entity = context.make("LegalEntity")
    name = row.pop("excluded_providers_entities_and_or_individuals")
    if name.startswith("Effective February"):
        return

    npi = row.pop("sanctioned_excluded_npi")
    entity.id = context.make_id(name, npi)
    entity.add("name", h.multi_split(name, [" aka ", " dba ", " DBA "]))
    entity.add("npiCode", npi.split("\n"))
    entity.add("country", "us")

    if associated_entity_name := row.pop("associated_legal_entity"):
        associated_entity = context.make("LegalEntity")
        associated_entity.id = context.make_id(associated_entity_name, entity.id)
        associated_entity.add("name", associated_entity_name.split(" aka "))
        associated_entity.add("country", "us")

        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, "related to", associated_entity.id)
        link.add("object", entity)
        link.add("subject", associated_entity)

        context.emit(associated_entity)
        context.emit(link)

    if controlling_interest_name := row.pop(
        "persons_with_controlling_interest_of_5_or_more"
    ):
        person = context.make("Person")
        person.id = context.make_id(controlling_interest_name, entity.id)
        person.add("name", controlling_interest_name.split(" aka "))
        person.add("country", "us")

        link = context.make("Ownership")
        link.id = context.make_id(entity.id, "own", person.id)
        link.add("asset", entity)
        link.add("owner", person)

        context.emit(link)
        context.emit(person)

    sanction = h.make_sanction(context, entity)
    sanction.add("provisions", f"Tier: {sanction_tier}")
    h.apply_dates(
        sanction, "startDate", row.pop("contract_termination_date").split("\n")
    )
    h.apply_date(
        sanction, "endDate", row.pop("nevada_medicaid_sanction_period_end_date")
    )

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "oig_exclusion_date",
            "oig_reinstate_date",
            "medicaid_provider",
            "nevada_medicaid_sanction_period",
            "provider_type",
        ],
    )


def page_settings(page: Page):
    # Find the bottom of the bottom-most rectangle on the page
    bottom = max(page.height - rect["y0"] for rect in page.rects)
    assert bottom < (page.height - 5), (bottom, page.height)
    return page, {"explicit_horizontal_lines": [bottom]}


def crawl_pdf_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='NV Exclusion List ']")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        headers_per_page=True,
        page_settings=page_settings,
    ):
        crawl_item(item, context)
