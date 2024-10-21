import pdfplumber
from pathlib import Path
from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from normality import collapse_spaces, slugify


def parse_pdf_table(
    context: Context, path: Path, save_debug_images=False, headers=None
):
    pdf = pdfplumber.open(path.as_posix())
    settings = {}
    for page_num, page in enumerate(pdf.pages, 1):
        # Find the bottom of the bottom-most rectangle on the page
        bottom = max(page.height - rect["y0"] for rect in page.rects)
        settings["explicit_horizontal_lines"] = [bottom]
        if save_debug_images:
            im = page.to_image()
            im.draw_hline(bottom, stroke=(0, 0, 255), stroke_width=1)
            im.draw_rects(page.find_table(settings).cells)
            im.save(f"page-{page_num}.png")
        assert bottom < (page.height - 5), (bottom, page.height)

        for row in page.extract_table(settings)[1:]:
            if headers is None:
                headers = [slugify(collapse_spaces(cell), sep="_") for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))


def crawl_item(row: Dict[str, str], context: Context):

    if first_name := row.pop("first_name"):
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("last_name"), first_name, row.get("exclusion_date")
        )
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=row.pop("last_name"),
            middle_name=row.pop("middle_initial"),
        )
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name"))
        entity.add("name", row.pop("last_name"))

    entity.add("sector", row.pop("provider_type"))
    entity.add("description", "Provider ID: " + row.pop("medicaid_provider_id"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("exclusion_date"))
    h.apply_date(sanction, "endDate", row.pop("reinstatement_date"))
    end_date = sanction.get("endDate")
    ended = end_date != [] and end_date[0] < context.data_time_iso
    if not ended:
        entity.add("topics", "debarment")

    context.emit(entity, target=not ended)
    context.emit(sanction)

    context.audit_data(row)


def crawl_pdf_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(".//a[contains(text(), 'Med Prov Excl-Rein List')]")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in parse_pdf_table(
        context,
        path,
        headers=[
            "last_name",
            "first_name",
            "middle_initial",
            "medicaid_provider_id",
            "provider_type",
            "exclusion_date",
            "reinstatement_date",
        ],
    ):
        crawl_item(item, context)
