from pathlib import Path
from typing import Dict
import re

from normality import collapse_spaces, slugify
import pdfplumber

from zavod import Context, helpers as h
from rigour.mime.types import PDF


from zavod.shed.gpt import run_image_prompt

prompt = """
 Extract structured data from the following page of a PDF document. Return 
 a JSON list (`providers`) in which each object represents an medical provider.
 Each object should have the following fields: `last_name`, `first_name`,
 `business_name`, `provider_type`, `provider_number`, `city`, `state`, `exclusion_date`.
 Return an empty string for unset fields.
"""

AKA_PATTERN = r"\ba\.?k\.?a[\. -]*"



def parse_pdf_table(context: Context, path: Path):
    headers = None
    pdf = pdfplumber.open(path.as_posix())
    options = {"join_y_tolerance": 100}
    for page_num, page in enumerate(pdf.pages, 1):
        for row_num, row in enumerate(page.extract_table(options), 1):
            im = page.to_image()
            im.draw_rects(page.find_table(options).cells)
            im.save(f"page-{page_num}.png")
            print(row)
            if headers is None:
                if row_num < 2:
                    continue
                headers = []
                for cell in row:
                    headers.append(slugify(collapse_spaces(cell), sep="_"))
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))



def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(
        context, city=row.pop("city"), state=row.pop("state"), country_code="US"
    )
    sector = row.pop("provider_type")
    provider_number = row.pop("provider_number")
    exclusion_date = row.pop("exclusion_date")
    last_name = row.pop("last_name")
    first_name = row.pop("first_name")
    business_name = row.pop("business_name")

    if not last_name and (first_name and business_name):
        context.log.info("Fixing shifted names", [last_name, first_name, business_name])
        last_name = first_name
        first_name = business_name

    if last_name:
        person = context.make("Person")
        person.id = context.make_id(row.get("first_name"), last_name, provider_number)
        h.apply_name(person, first_name=first_name, last_name=last_name)
        h.apply_address(context, person, address)
        person.add("sector", sector)
        person.add("description", "Provider Number: " + provider_number)
        person.add("topics", "debarment")
        person.add("country", "us")

        sanction = h.make_sanction(context, person)
        h.apply_date(sanction, "startDate", exclusion_date)

        context.emit(person, target=True)
        context.emit(sanction)

    is_business = False

    if business_name:
        if re.match(AKA_PATTERN, business_name, flags=re.IGNORECASE):
            cleaned_name = re.sub(AKA_PATTERN, "", business_name, flags=re.IGNORECASE)
            for name in cleaned_name.split(","):
                person.add("alias", name)

        else:
            is_business = True
            company = context.make("Company")
            company.id = context.make_id(business_name, provider_number)
            company.add("name", business_name)
            h.apply_address(context, company, address)
            company.add("sector", sector)
            company.add("description", "Provider Number: " + provider_number)
            company.add("topics", "debarment")
            company.add("country", "us")

            company_sanction = h.make_sanction(context, company)
            h.apply_date(company_sanction, "startDate", exclusion_date)

            context.emit(company, target=True)
            context.emit(company_sanction)

    if last_name and is_business:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)

        context.emit(link)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    return doc.xpath(".//a[text()='Wyoming Provider Exclusion List ']")[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_excel_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in parse_pdf_table(context, path):
        print(item)
        crawl_item(item, context)
