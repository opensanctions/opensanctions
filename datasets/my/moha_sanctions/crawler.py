import re
from normality import slugify
from banal import ensure_list, first
from urllib.parse import urljoin
from rigour.mime.types import PDF
from typing import Dict, List, Optional, Any

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html
from zavod.shed.gpt import run_image_prompt


# FORMATS = ["%d %B %Y", "%d.%m.%Y"]
# MONTHS = {"Mei": "May", "Februari": "February", "Ogos": "August"}

PROMPT = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`rows`) in which each object represents a row in the table.
Each object should have one field for each column in the table, named using 
the exact text in the column header but leaving out the column numbers
in brackets. If multiple values exist in a cell, provide an array with each
of the values in the cell separated as strings.
"""


def clean_row(row: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for key, values in row.items():
        clean_key = slugify(key, sep="_")
        if clean_key is None:
            continue
        if not isinstance(values, list):
            values = [values]
        cleaned: List[str] = []
        for val in values:
            if val is None:
                continue
            val = str(val).strip()
            if val == "-":
                continue
            val_ = re.sub(r"^[a-z]\) ", " ", val)
            cleaned.append(val_.strip())
        if len(cleaned):
            out[clean_key] = cleaned
    return out


# def parse_date(date: Optional[str]) -> List[str]:
#     if date is None:
#         return []
#     for malay, eng in MONTHS.items():
#         date = date.replace(malay, eng)
#     return h.parse_date(date, FORMATS)


def parse_date(date: Optional[str], context: Context) -> List[str]:
    if date is None:
        return []

    date = date.lower().strip()
    date = h.replace_months(context.dataset, date)
    date_info = h.parse_formats(date, context.dataset.dates.formats)
    if date_info and date_info.dt:
        return date_info.text

    context.log.warning("Failed to parse date", raw_date=date)
    return []


def unblock_validator(el) -> bool:
    return "SANCTION LIST MADE BY THE MINISTRY OF HOME AFFAIRS" in el.text_content()


def crawl_person(context: Context, data: Dict[str, List[str]]):
    entity = context.make("Person")
    code = first(data.pop("rujukan", []))
    code = code or first(data.pop("no_rujukan", []))
    if code is None:
        context.log.warn("No code for person", data=data)
        return
    entity.id = context.make_slug("person", code)
    for name in ensure_list(data.pop("nama")):
        entity.add("name", name.split("@"))
    entity.add("topics", "sanction")
    for fld in ["tarikh_lahir", "tarikhlahir"]:
        for dob in ensure_list(data.pop(fld, [])):
            entity.add("birthDate", parse_date(dob, context))
    entity.add("birthPlace", data.pop("tempat_lahir", []))

    entity.add("alias", data.pop("nama_lain", []))
    entity.add("nationality", data.pop("warganegara", []))

    entity.add("passportNumber", data.pop("nombor_pasport", []))
    entity.add("idNumber", data.pop("nombor_kad_pengenalan", []))
    entity.add("address", data.pop("alamat", []))

    sanction = h.make_sanction(context, entity)
    for fld in [
        "tarikh_disenaraikan",
        "tarikh_disenaraiaken",
        "tarikh_disenarai",
        "tarikh_disenaraihan",
    ]:
        for date in ensure_list(data.pop(fld, [])):
            sanction.add("listingDate", parse_date(date, context))
    sanction.add("authorityId", code)

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(data, ignore=["no"])


def crawl_group(context: Context, data: Dict[str, List[str]]):
    entity = context.make("Organization")
    code = first(data.pop("no_ruj"))
    if code is None:
        context.log.warn("No code for group", data=data)
        return
    entity.id = context.make_slug("group", code)
    entity.add("name", data.pop("nama"))
    entity.add("topics", "sanction")
    entity.add("alias", data.pop("alias", []))
    entity.add("country", data.pop("alamat", []))
    entity.add("weakAlias", data.pop("nama_lain", []))

    sanction = h.make_sanction(context, entity)
    for date in ensure_list(data.pop("tarikh_disenaraikan", [])):
        sanction.add("listingDate", parse_date(date, context))
    sanction.add("authorityId", code)

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(data, ignore=["no"])


def crawl_pdf_url(context: Context) -> str:
    html = fetch_html(context, context.data_url, unblock_validator, cache_days=5)
    for a in html.findall('.//div[@class="uk-container"]//a'):
        if ".pdf" in a.get("href", ""):
            return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    path = context.fetch_resource("source.pdf", pdf_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, PROMPT, page_path)
        for row in data["rows"]:
            norm = clean_row(row)
            if "rujukan" in norm and "warganegara" in norm:
                crawl_person(context, norm)
            elif "no_rujukan" in norm and "warganegara" in norm:
                crawl_person(context, norm)
            elif "no_ruj" in norm:
                crawl_group(context, norm)
            else:
                context.log.warn(
                    "Cannot identify row format", row=row, page=page_path.name
                )
