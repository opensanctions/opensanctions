from zavod import Context
from typing import Dict, List, Optional
from rigour.mime.types import XLSX
import openpyxl
from normality import slugify
from zavod import helpers as h
import shutil
from pprint import pprint as pp
import re


def extract_passport_no(text):
    if not text:
        return None
    pattern = r"\b[A-Z0-9]{5,}\b"
    matches = re.findall(pattern, text)

    return matches


def clean_address(text):
    if not text:
        return None

    # regex to match entries wih pattern from a) to z)
    patterns = [
        r"\b[a-z]\)\s(.*?)(?=\s[a-z]\)|$)",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL | re.VERBOSE)
        if matches:
            return [match.strip(", ") for match in matches]
    else:
        return text


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.iter_rows(min_row=0):
        cells = [c.value for c in row]

        if headers is None and all(cells):
            headers = [h for h in cells]
            continue

        if all(cells) and len(cells) == len(headers):
            if 'מס"ד' in cells:
                continue

            yield cells


def parse_sheet_row(context: Context, row: Dict[str, str]):
    assert (
        len(row) == 11
    ), "Crawler was developed with excel sheet of 11 columns, please check that attributes still conforms to index used"

    person = context.make("Person")

    other_info = row.pop(10)
    address_or_nationality = row.pop(9)
    dob = row.pop(8)
    passport = row.pop(7)
    name = row.pop(6)
    isreal_adoption_date = row.pop(5)  # permanent
    isreal_temp_adoption_date = row.pop(4)
    serial_no = row.pop(3)
    originally_declared_by = row.pop(2)
    declaration_date = row.pop(1)
    if declaration_date:
        declaration_date = h.parse_date(declaration_date, "%d %b. %Y")
    record_id = row.pop(0)

    pp(
        {
            "other_info": other_info,
            "address": clean_address(address_or_nationality),
            "dob": dob,
            "passport": extract_passport_no(passport),
            "name": name,
            "isreal_adoption_date": isreal_adoption_date,
            "isreal_temp_adoption_date": isreal_temp_adoption_date,
            "serial_no": serial_no,
            "originally_declared_by": originally_declared_by,
            "declaration_date": declaration_date,
            "record_id": record_id,
        }
    )
    person.id = context.make_id("Person", record_id)
    person.add("name", name)
    person.add("notes", other_info)
    person.add("passportNumber", extract_passport_no(passport))
    person.add("nationality", address_or_nationality)
    person.add("address", address_or_nationality)

    sanction = h.make_sanction(context, person)
    sanction.add("authority", originally_declared_by)
    sanction.add("unscId", serial_no)
    sanction.add("listingDate", declaration_date)
    # sanction.add("reason", reason)
    # sanction.add("provisions", legal_basis)
    # sanction.add("program", sanction_regime)
    # sanction.add("endDate", expiration_date)

    context.emit(person, target=True)
    context.emit(sanction)


def crawl(context: Context):
    assert context.dataset.base_path is not None

    data_path = context.dataset.base_path / "data.xlsx"
    source_path = context.get_resource_path("source.xlsx")

    shutil.copyfile(data_path, source_path)

    context.export_resource(source_path, XLSX, title=context.SOURCE_TITLE)
    rows = sheet_to_dicts(
        openpyxl.load_workbook(source_path, read_only=True).worksheets[0]
    )

    for row in rows:
        # print("\n============".join(row.keys()))
        # break
        # print(row)
        # print("+=============")
        # break
        parse_sheet_row(context, row)
        # print("\n============")
