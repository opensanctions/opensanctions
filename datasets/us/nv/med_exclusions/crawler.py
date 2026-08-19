from typing import Any
from normality import slugify
from rigour.mime.types import PDF
from pdfplumber.page import Page

from zavod import Context, helpers as h
from zavod.extract import zyte_api

CONTROLLING_INTEREST = "persons_with_controlling_interest_of_5_or_more"


def crawl_item(
    row: dict[str, str | None], context: Context, person_names: set[str]
) -> None:
    # We already crawl the federal dataset on another crawler
    sanction_tier = row.pop("nevada_medicaid_sanction_tier")
    if (sanction_tier or "").lower() == "federal":
        return

    entity = context.make("LegalEntity")
    name = row.pop("excluded_providers_entities_and_or_individuals")
    if (name or "").startswith("Effective February"):
        return

    npi = row.pop("sanctioned_excluded_npi")
    entity.id = context.make_id(name, npi)
    names = h.multi_split(name, [" aka ", " dba ", " DBA "])
    entity.add("name", names)
    entity.add("npiCode", (npi or "").split("\n"))
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

    controlling_interest_name = row.pop(CONTROLLING_INTEREST)
    # A provider the list names as a controlling-interest holder is a practitioner:
    # nothing to own, and making them a Company would leave them unassemblable once
    # resolution merges them with the Person another row emits.
    is_individual = any(slugify(n) in person_names for n in names)
    if controlling_interest_name and not is_individual:
        entity.add_schema("Company")

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
        sanction, "startDate", (row.pop("contract_termination_date") or "").split("\n")
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


def page_settings(page: Page) -> tuple[Page, dict[str, Any]]:
    # Find the bottom of the bottom-most rectangle on the page
    bottom = max(page.height - rect["y0"] for rect in page.rects)
    assert bottom < (page.height - 5), (bottom, page.height)
    return page, {"explicit_horizontal_lines": [bottom]}


def crawl_pdf_url(context: Context) -> str:
    pdf_link_xpath = "//a[normalize-space()='NV Exclusion List']"
    doc = zyte_api.fetch_html(
        context, context.data_url, pdf_link_xpath, geolocation="US", absolute_links=True
    )
    url = h.xpath_string(doc, pdf_link_xpath + "/@href")
    assert url is not None, "Could not find PDF URL"
    return url


def crawl(context: Context) -> None:
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.pdf", crawl_pdf_url(context), PDF, geolocation="US"
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    rows = list(
        h.parse_pdf_table(
            context,
            path,
            headers_per_page=True,
            page_settings=page_settings,
        )
    )
    # The list marks a name as a person by putting it in this column, on any row.
    person_names = {
        slug
        for row in rows
        for part in (row.get(CONTROLLING_INTEREST) or "").split("\n")
        for name in part.split(" aka ")
        if (slug := slugify(name)) is not None
    }
    for row in rows:
        crawl_item(row, context, person_names)
