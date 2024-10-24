import pdfplumber
import re
from pathlib import Path
from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from normality import collapse_spaces, slugify

AKA_SPLIT = r"\baka\b|\ba\.k\.a\b|\bAKA\b|\bor\b"


def parse_pdf_table(context: Context, path: Path, save_debug_images=False):
    pdf = pdfplumber.open(path.as_posix())
    settings = {}
    for page_num, page in enumerate(pdf.pages, 1):
        if save_debug_images:
            im = page.to_image()
            im.save(f"page-{page_num}.png")
        headers = None
        for row in page.extract_table(settings):
            if headers is None:
                headers = [slugify(collapse_spaces(cell), sep="_") for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            values = [collapse_spaces(cell) for cell in row]
            yield dict(zip(headers, values))


def crawl_item(row: Dict[str, str], context: Context):

    if raw_first_name := row.pop("first_name"):
        entity = context.make("Person")

        raw_last_name = row.pop("last_name_or_business_name")
        raw_middle_initial = row.pop("middle_initial")

        entity.id = context.make_id(
            raw_last_name, raw_middle_initial, raw_first_name, row.get("exclusion_date")
        )

        # There is some cases where there is a business name with "DBA"
        if " DBA " in raw_last_name:
            raw_last_name, business_name = raw_last_name.split(" DBA ")
            company = context.make("Company")
            company.id = context.make_id(business_name)
            company.add("name", business_name)
            link = context.make("UnknownLink")
            link.id = context.make_id(entity.id, company.id)
            link.add("object", entity)
            link.add("subject", company)
            link.add("role", "d/b/a")
            context.emit(company)
            context.emit(link)

        for first_name in re.split(AKA_SPLIT, raw_first_name):
            for last_name in re.split(AKA_SPLIT, raw_last_name):
                for middle_initial in re.split(AKA_SPLIT, raw_middle_initial):
                    h.apply_name(
                        entity,
                        first_name=first_name,
                        last_name=last_name,
                        middle_name=middle_initial,
                    )
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name_or_business_name"))
        entity.add("name", row.pop("last_name_or_business_name"))

    entity.add("sector", row.pop("last_known_program_or_provider_type"))
    if row.get("medicaid_provider_id") != "NONE":
        entity.add("description", "Provider ID: " + row.pop("medicaid_provider_id"))
    else:
        row.pop("medicaid_provider_id")
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
    for item in parse_pdf_table(context, path):
        print(item)
        crawl_item(item, context)
