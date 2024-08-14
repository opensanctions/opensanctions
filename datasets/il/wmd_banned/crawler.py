from zavod import Context
from typing import Dict, List, Optional
from rigour.mime.types import XLSX
import openpyxl
from zavod import helpers as h
import shutil
from pprint import pprint as pp
import re


def extract_passport_no(text):
    if not text:
        return None
    text = str(text)
    pattern = r"\b[A-Z0-9]{5,}\b"
    matches = re.findall(pattern, text)

    return matches


def extract_n_pop_address(text):
    if not text:
        return None, None

    pattern = r"(address|location):\s*(.*)"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        address = match.group(2).strip()

        # Remove the extracted location from the original text
        updated_text = re.sub(
            pattern, "", text, flags=re.DOTALL | re.IGNORECASE
        ).strip()

        return address, updated_text

    else:
        return None, text


def format_numbered_listings(text):
    if not text:
        return None
    patterns = [
        r"\b[a-z]\)\s(.*?)(?=\s[a-z]\)|$)",  # Matches 'a) text b) text'
        r"\b\d+:\s(.*?)(?=\s\d+:|$)",  # Matches '1: text 2: text'
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.DOTALL | re.VERBOSE | re.IGNORECASE)
        if matches:
            return list(
                set(
                    [
                        match.replace("\n", "").replace("'", "").strip()
                        for match in matches
                    ]
                )
            )
    else:
        return text


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.iter_rows(min_row=0):
        cells = [c.value for c in row]

        if headers is None:
            if all(cells):
                headers = [h for h in cells]
            continue

        if len(cells) == len(headers):
            if 'מס"ד' in cells:  # cell that contains headers
                continue

            none_count = sum(1 for item in cells if not item)
            if none_count > len(headers) // 2:  # headers with one or two elements
                continue

            yield cells


def parse_sheet_row(context: Context, row: Dict[str, str]):
    assert (
        len(row) == 11
    ), "Crawler was developed with excel sheet of 11 columns, please check that attributes still conforms to index used"
    row_copy = row.copy()

    other_info = row.pop(10)
    address_nationality = row.pop(9)

    # Extract address if it exists from either the info or nationality attibrutes
    info_address, notes = extract_n_pop_address(other_info)
    nat_address, nationality = extract_n_pop_address(address_nationality)
    address = info_address or nat_address

    dob = row.pop(8)
    # if dob:
    #     dob = h.parse_date(dob, "%d %b. %Y")
    passport = row.pop(7)
    parse_name = row.pop(6)
    name_split = re.split(r"\bA\.K\.A\b|;", parse_name, flags=re.IGNORECASE)
    name = format_numbered_listings(name_split[0].strip())
    alias = [format_numbered_listings(alias) for alias in name_split[1:]]

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
            # "other_info": other_info,
            # "notes": notes,
            # "extracted_address": address,
            # "ori_address": address or address_nationality,
            # "clean_ori_address": format_numbered_listings(address),
            # "nationality": format_numbered_listings(nationality),
            # "dob": dob,
            # "passport_ori": passport,
            # "passport": extract_passport_no(passport),
            "parse_name": parse_name,
            "name": name,
            "alias": alias,
            # "isreal_adoption_date": isreal_adoption_date,
            # "isreal_temp_adoption_date": isreal_temp_adoption_date,
            # "serial_no": serial_no,
            # "originally_declared_by": originally_declared_by,
            # "declaration_date": declaration_date,
            # "record_id": record_id,
            # "add_nationality": address_nationality,
        }
    )

    if "iri" in serial_no.lower() or "pi" in serial_no.lower():
        entity = context.make("Person")
        entity.id = context.make_id("Person", f"{record_id}-{serial_no}")
        entity.add("passportNumber", extract_passport_no(passport))
        entity.add("nationality", nationality)
        # entity.add("birthDate", dob)

    elif "ire" in serial_no.lower() or "pe" in serial_no.lower():
        entity = context.make("Organization")
        entity.id = context.make_id("Company", f"{record_id}-{serial_no}")

    else:
        context.log.warn(f"Entity not recognized from serial number:{serial_no}")

    entity.add("name", name)
    entity.add("alias", alias)
    entity.add("notes", notes)
    entity.add("address", format_numbered_listings(address))

    sanction = h.make_sanction(context, entity)
    sanction.add("authority", originally_declared_by, lang="he")
    sanction.add("unscId", serial_no)
    sanction.add("listingDate", declaration_date)
    # sanction.add("reason", reason)
    # sanction.add("provisions", legal_basis)
    # sanction.add("program", sanction_regime)
    # sanction.add("endDate", expiration_date)

    context.emit(entity, target=True)
    context.emit(sanction)
    # print("===========")


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
        parse_sheet_row(context, row)
        # print("\n============")
