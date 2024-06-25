from typing import Generator, Dict, Tuple, Optional
from lxml.etree import _Element
from normality import slugify
from zavod import Context, helpers as h
import re
from typing import Generator
import openpyxl
from openpyxl import load_workbook
from pantomime.types import XLSX
from normality import stringify, slugify
from datetime import datetime
from rigour.names import pick_name

HEADERS = {
    "User-Agent": "AAA",
}


def arabic_to_western(arabic_date):
    arabic_numerals = "٠١٢٣٤٥٦٧٨٩"
    western_numerals = "0123456789"
    transtable = str.maketrans(arabic_numerals, western_numerals)
    return arabic_date.translate(transtable)


def parse_date(date_str: str) -> datetime:

    if not date_str:
        return None

    western_date = arabic_to_western(date_str)

    # We first check if it's just one date
    try:
        return datetime.strptime(western_date, "%Y/%m/%d")
    except ValueError:
        # Otherwise we check if the first line is a date
        try:
            return datetime.strptime(western_date.split("\n")[0], "%d/%m/%Y")
        except ValueError:
            return None


def parse_sheet(
    sheet: openpyxl.worksheet.worksheet.Worksheet,
    skiprows: int = 0,
) -> Generator[dict, None, None]:
    headers = None

    row_counter = 0

    for row in sheet.iter_rows():
        # Increment row counter
        row_counter += 1

        # Skip the desired number of rows
        if row_counter <= skiprows:
            continue
        cells = [c.value for c in row]
        if headers is None:
            headers = []
            for idx, cell in enumerate(cells):
                if cell is None:
                    cell = f"column_{idx}"
                headers.append(slugify(cell, "_").lower())
            continue

        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            record[header] = stringify(value)
        if len(record) == 0:
            continue
        yield record


def crawl_terrorist(input_dict: dict, context: Context):

    first_name = input_dict.pop("alasm")
    last_name = input_dict.pop("asm_akhr")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("nationality", input_dict.pop("aljnsyt"), lang="ar")
    person.add("passportNumber", input_dict.pop("jwaz_alsfr"))
    person.add("idNumber", input_dict.pop("alrqm_alqwmy"))
    person.add("topics", "crime.terror")

    sanction = h.make_sanction(context, person)

    sanction.add("listingDate", parse_date(input_dict.pop("tarykh_alnshr")))
    sanction.add("recordId", input_dict.pop("rqm_alqdyt"))
    sanction.add("authorityId", input_dict.pop("rqm_qrar_adraj_alarhabyyn"))
    sanction.add(
        "summary",
        "Publication Page: {}".format(input_dict.pop("dd_alnshr")),
    )

    context.emit(person, target=True)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl_terrorist_entities(input_dict: dict, context: Context):

    name = input_dict.pop("asm_alkyan")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "crime.terror")

    sanction = h.make_sanction(context, entity)
    sanction.add("recordId", input_dict.pop("rqm_alqdyt"))
    sanction.add("summary", input_dict.pop("althdythat"), lang="ar")

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(input_dict, ignore=["mslsl", "aldd_baljrydt_alrsmyt"])


def crawl_legal_persons(input_dict: dict, context: Context):

    name = input_dict.pop("asm_alshkhs_alatbary")

    if not name:
        return

    company = context.make("Company")
    company.id = context.make_id(name)
    company.add("name", name)

    company.add("address", input_dict.pop("almqr"))
    company.add("notes", input_dict.pop("rqm_alsjl_altjary_mshhrt_brqm"))
    company.add("topics", "crime.terror")

    sanction = h.make_sanction(context, company)
    sanction.add("recordId", input_dict.pop("rqm_alqdyt"))
    sanction.add("summary", input_dict.pop("althdythat"), lang="ar")

    context.emit(company, target=True)
    context.emit(sanction)
    context.audit_data(input_dict, ignore=["mslsl", "aldd_baljrydt_alrsmyt"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url, headers=HEADERS)
    response.make_links_absolute(context.data_url)

    excel_link = response.find(".//*[@class='LinkStyle AutoDownload']").get("href")

    path = context.fetch_resource("list.xlsx", excel_link)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in parse_sheet(wb["الإرهابيين"]):
        crawl_terrorist(item, context)

    for item in parse_sheet(wb["الكيانات الإرهابية"], skiprows=1):
        crawl_terrorist_entities(item, context)

    for item in parse_sheet(wb["الشخصيات الاعتبارية"], skiprows=1):
        crawl_legal_persons(item, context)
