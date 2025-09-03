import string

from urllib.parse import urljoin
from rigour.mime.types import PDF
from typing import Tuple, Dict, Any
from pdfplumber.page import Page

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ID_SPLITS = [f"({c}) " for c in string.ascii_lowercase[:17]]  # a-q
ALIAS_SPLITS = [f"{c}) " for c in string.ascii_lowercase[:17]]  # a-q


def rename_headers(context, entry, custom_lookup):
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value(custom_lookup, old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def clean_value(v):
    if not v:
        return v
    v = v.replace("\n", " ").strip()
    return "" if v == "-" else v


def crawl_row(context: Context, row, schema, key) -> None:
    names = row.pop("name")
    reference = row.pop("reference_no")
    # Early exits for headers or invalid rows
    if not names or "Nama" in names:
        return
    entity = context.make(schema)
    entity.id = context.make_slug(key, reference)
    entity.add("name", names.split("@"))
    entity.add("topics", "sanction")
    entity.add("address", row.pop("address"))

    alias_splits = ID_SPLITS if entity.schema.is_a("Person") else ALIAS_SPLITS
    for field in ["alias", "other_name"]:
        value = row.pop(field, None)
        if value:
            entity.add("alias", h.multi_split(value, alias_splits))

    if entity.schema.is_a("Person"):
        entity.add("title", row.pop("title", None))
        h.apply_date(entity, "birthDate", row.pop("birth_date", None))
        entity.add("birthPlace", row.pop("birth_place", None))
        entity.add("nationality", row.pop("citizenship", None))
        entity.add("position", row.pop("position", None))
        entity.add("passportNumber", row.pop("passport_no", None))
        for id in h.multi_split(row.pop("id_no", None), ID_SPLITS):
            entity.add("idNumber", id)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", row.pop("date_listed", None))
    sanction.add("authorityId", reference)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["no", "False"])


def page_settings(page: Page) -> Tuple[Page, Dict[str, Any]]:
    # Crop top/bottom margins to focus on table area
    cropped = page.crop((0, 75, page.width, page.height - 10))
    settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "text_tolerance": 1,
    }
    return cropped, settings


def crawl_pdf_url(context: Context) -> str:
    validator = ".//*[contains(text(), 'LIST OF SANCTIONS UNDER THE MINISTRY OF HOME AFFAIRS (MOHA)')]"
    html = fetch_html(context, context.data_url, validator, cache_days=5)
    for a in html.findall('.//div[@class="uk-container"]//a'):
        if "sanctions list" not in a.text_content().lower():
            continue
        if ".pdf" in a.get("href", ""):
            return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    path = context.fetch_resource("source.pdf", pdf_url)
    # If the PDF file has changed, check if the headers mappings are still on
    h.assert_file_hash(path, "39579602793184083f2f28bfa5ecc0eaaa08a7af")
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for row in h.parse_pdf_table(
        context,
        path,
        headers_per_page=True,
        preserve_header_newlines=True,
        skiprows=0,
        page_settings=page_settings,
    ):
        row = {k: clean_value(v) for k, v in row.items()}
        # Decide schema based on number of columns in header
        num_cols = len(row.keys())
        if num_cols == 13:
            schema, key = "Person", "person"
            row = rename_headers(context, row, "columns_person")
        elif num_cols == 7:
            schema, key = "Organization", "group"
            row = rename_headers(context, row, "columns_organization")
        else:
            context.log.warning(f"Unexpected number of columns: {num_cols}")
        crawl_row(context, row, schema, key)
