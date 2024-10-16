from pathlib import Path
from typing import Dict
from normality import collapse_spaces, slugify
from rigour.mime.types import PDF
import pdfplumber

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    entity = context.make("LegalEntity")
    name = row.pop("excluded_providers_entities_and_or_individuals")
    npi = row.pop("sanctioned_exclude_d_npi")
    entity.id = context.make_id(name, npi)
    entity.add("name", name)
    entity.add("npiCode", npi)
    entity.add("country", "us")

    if associated_entity_name := row.pop("associated_legal_entity"):
        associated_entity = context.make("LegalEntity")
        associated_entity.id = context.make_id(associated_entity_name, entity.id)
        associated_entity.add("name", associated_entity_name)
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
        person.add("name", controlling_interest_name)
        person.add("country", "us")

        link = context.make("Ownership")
        link.id = context.make_id(entity.id, "own", person.id)
        link.add("asset", entity)
        link.add("owner", person)

        context.emit(link)
        context.emit(person)

    sanction = h.make_sanction(context, entity)
    sanction.add("provisions", f'Tier: {row.pop("nevada_medicaid_sanction_tier")}')
    h.apply_dates(
        sanction, "startDate", row.pop("contract_termination_date").split("\n")
    )
    ended = False
    h.apply_date(
        sanction, "endDate", row.pop("nevada_medicaid_sanction_period_end_date")
    )
    end_date = sanction.get("endDate")
    ended = end_date != [] and end_date[0] < context.data_time_iso
    if not ended:
        entity.add("topics", "debarment")

    context.emit(entity, target=not ended)
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


def parse_pdf_table(context: Context, path: Path, save_debug_images=False):
    pdf = pdfplumber.open(path.as_posix())
    settings = {}
    for page_num, page in enumerate(pdf.pages, 1):
        bottom = max(page.height - rect["y0"] for rect in page.rects)
        if save_debug_images:
            im = page.to_image()
            settings["explicit_horizontal_lines"] = [bottom]
            im.draw_hline(bottom, stroke=(0, 0, 255), stroke_width=1)
            im.draw_rects(page.find_table(settings).cells)
            im.save(f"page-{page_num}.png")
        assert bottom < (page.height - 5), (bottom, page.height)
        headers = None
        for row in page.extract_table(settings):
            if headers is None:
                headers = [slugify(collapse_spaces(cell), sep="_") for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))


def crawl_pdf_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='NV Exclusion List ']")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in parse_pdf_table(context, path):
        crawl_item(item, context)
