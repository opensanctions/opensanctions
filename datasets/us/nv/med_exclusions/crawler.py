from typing import Any
from normality import slugify
from rigour.mime.types import PDF
from rigour.names import extract_org_types
from pdfplumber.page import Page

from zavod import Context, helpers as h
from zavod.extract import zyte_api

CONTROLLING_INTEREST = "persons_with_controlling_interest_of_5_or_more"
# rigour reads each of these as a legal form, but on a medical provider they are a
# generational suffix or a credential: "Claude Arthur Verbal II", "Gary Ridenour OD".
PERSON_SUFFIXES = {"ii", "iii", "iv", "od", "pt"}


def is_business(name: str) -> bool:
    """Whether a name carries a legal form, which a person's name doesn't."""
    return any(norm not in PERSON_SUFFIXES for _, norm in extract_org_types(name))


def controlling_interest_holders(context: Context, value: str | None) -> list[str]:
    """Split a controlling-interest cell into one name per holder.

    A cell names one holder per line, except where a line break falls inside a name
    or where two names run together with no separator at all. The lookup spells those
    out, since nothing in the cell distinguishes them.
    """
    if not value:
        return []
    result = context.lookup("controlling_interest", value)
    names = result.values if result is not None else value.split("\n")
    return [name.lstrip("&").strip() for name in names if name.strip("& ")]


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

    holders = controlling_interest_holders(context, row.pop(CONTROLLING_INTEREST))
    # A provider the list names as a controlling-interest holder is a practitioner:
    # nothing to own, and making them a Company would leave them unassemblable once
    # resolution merges them with the Person another row emits.
    is_individual = any(slugify(n) in person_names for n in names)
    if holders and not is_individual:
        entity.add_schema("Company")

        for holder in holders:
            # The column says persons, but a couple of cells name a business.
            owner = context.make("Company" if is_business(holder) else "Person")
            owner.id = context.make_id(holder, entity.id)
            owner.add("name", holder)
            owner.add("country", "us")

            link = context.make("Ownership")
            link.id = context.make_id(entity.id, "own", owner.id)
            link.add("asset", entity)
            link.add("owner", owner)

            context.emit(link)
            context.emit(owner)

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
        for name in controlling_interest_holders(context, row.get(CONTROLLING_INTEREST))
        if not is_business(name) and (slug := slugify(name)) is not None
    }
    for row in rows:
        crawl_item(row, context, person_names)
