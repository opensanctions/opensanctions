from pathlib import Path
from typing import Dict
from normality import collapse_spaces, slugify
import pdfplumber
from rigour.mime.types import PDF

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if first_name := row.pop("first_name"):
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("last_name"), first_name, row.get("exclusion_date")
        )
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=row.pop("last_name_or_business_name"),
            middle_name=row.pop("middle_initial"),
        )
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name_or_business_name"))
        entity.add("name", row.pop("last_name_or_business_name"))

    entity.add("sector", row.pop("last_known_program_or_provider_type"))
    entity.add("description", "Provider ID: " + row.pop("medicaid_provider_id"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("exclusion_date"))
    if row.get("reinstatement_date") != "Indefinite":
        h.apply_date(sanction, "endDate", row.pop("reinstatement_date"))
        is_debarred = False
    else:
        row.pop("reinstatement_date")
        is_debarred = True
        entity.add("topics", "debarment")

    context.emit(entity, target=is_debarred)
    context.emit(sanction)

    context.audit_data(row)


def parse_pdf_table(context: Context, path: Path, save_debug_images=False):
    pdf = pdfplumber.open(path.as_posix())
    settings = {}
    for page_num, page in enumerate(pdf.pages, 1):
        if save_debug_images:
            im = page.to_image()
            im.draw_rects(page.find_table(settings).cells)
            im.save(f"page-{page_num}.png")

        headers = None
        for row in page.extract_table(settings):
            if headers is None:
                headers = [slugify(collapse_spaces(cell), sep="_") for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in parse_pdf_table(context, path):
        crawl_item(item, context)
