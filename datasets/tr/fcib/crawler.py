from datetime import datetime, date
from lxml import etree
from normality import collapse_spaces, stringify, slugify
from openpyxl import load_workbook
from typing import Dict, Iterable
from urllib.parse import urljoin
import csv

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

DATE_FORMAT = "%d.%m.%Y"

MAIN_URL = "https://en.hmb.gov.tr"

DOCX_LINK = "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/A-6415-SAYILI-KANUN-5.-MADDE.docx"  # original link for assertion
CSV_LINK = "https://docs.google.com/spreadsheets/d/1SFH2gKt2gFVCNvl2wnNuFZT3m-iVNYlXiWHXRquddFI/pub?gid=594686664&single=true&output=csv"  # Google Sheets link based on the original link


XLSX_LINK = [
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/B-YABANCI-ULKE-TALEPLERINE-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-6.-MADDE.xlsx",
        "Asset freezes pursuant to Article 6 of Law No. 6415, targeting individuals and entities based on requests made by foreign governments.",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/06/C-6415-SAYILI-KANUN-7.-MADDE.xlsx",
        "Asset freezes pursuant to Article 7 of Law No. 6415, targeting individuals and entities through domestic legal actions and decisions.",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/D-7262-SAYILI-KANUN-3.A-VE-3.B-MADDELERI.xlsx",
        "Asset freezes within the scope of Articles 3.A and 3.B of Law No. 7262, aimed at preventing the financing of proliferation of weapons of mass destruction.",
    ),
]

SPLITS = [
    "a)",
    "b)",
    "c)",
    "d)",
    "e)",
    "f)",
    "g)",
    "h)",
    "i)",
    "j)",
    "k)",
    "l)",
    "m)",
    "n)",
]

NEW_LINE_SPLIT = ["\n"]

ADDRESS_SPLITS = (
    [
        "Şube 1:",
        "Şube 2:",
        "Şube 3:",
        "Şube 4:",
        "Şube 5:",
        "Şube 6:",
        "Şube 7:",
        "Şube 8:",
        "Şube 9:",
        "Şube 10:",
        "Şube 11:",
    ]
    + SPLITS
    + NEW_LINE_SPLIT
)


def clean_row(row: Dict[str, str]) -> Dict[str, str]:
    """Clean non-standard spaces from row keys and values."""
    return {
        k.replace("\xa0", " ").strip(): v.replace("\xa0", " ").strip()
        for k, v in row.items()
    }


def normalize_header(header: str) -> str:
    """Normalize header strings by collapsing spaces and removing newlines."""
    if header is None:
        return None
    # Replace newlines and tabs with spaces, then collapse multiple spaces into one
    header = header.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return collapse_spaces(header).strip()


def str_cell(cell: object) -> str:
    """Convert a cell value to string, handling dates."""
    if cell is None:
        return ""
    if isinstance(cell, (datetime, date)):
        return cell.isoformat()
    return str(cell)


def crawl_row(context: Context, row: Dict[str, str], program: str):
    name = row.pop("name")
    if not name:
        return  # in the XLSX file, there are empty rows

    alias = row.pop("alias")
    previous_name = row.pop("previous_name", "")
    internal_id = row.pop("sequence_no")
    pass_no = row.pop("passport_number", "")  # Person
    identifier = row.pop("passport_number_other_info", "")  # LegalEntity
    nationality_country = row.pop("nationality_country", "")
    legal_entity_name = row.pop("legal_entity_name", "")  # LegalEntity
    birth_establishment_date = h.parse_date(
        row.pop("date_of_birth_establishment", ""), DATE_FORMAT
    )  # LegalEntity
    birth_place = row.pop("birth_place", "")  # Person
    birth_date = h.parse_date(row.pop("birth_date", ""), DATE_FORMAT)  # Person
    position = row.pop("position", "")
    address = row.pop("address", "")
    notes = row.pop("other_information", "")
    organization = row.pop("organization", "")
    sanction_type = row.pop("sanction_type", "")
    listing_date = row.pop("listing_date", "")
    # official_gazette_date = row.pop("official_gazette_date", "")

    if birth_date or birth_place:
        person = context.make("Person")
        person.id = context.make_id(name, internal_id)
        person.add("name", name)
        person.add("alias", h.multi_split(alias, SPLITS))
        person.add("nationality", nationality_country)
        person.add("previousName", previous_name)
        person.add("birthPlace", birth_place)
        person.add("birthDate", h.multi_split(birth_date, SPLITS))
        person.add("birthDate", birth_establishment_date)
        person.add("passportNumber", pass_no)
        person.add("position", position)
        person.add("address", h.multi_split(address, SPLITS))
        person.add("notes", notes)

        sanction = h.make_sanction(context, person)
        sanction.add("description", sanction_type)
        sanction.add("reason", organization)
        sanction.add("program", program)  # depends on the xlsx file
        sanction.add("listingDate", listing_date)

        context.emit(person)
        context.emit(sanction)
    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name)
        entity.add("name", name)
        entity.add("name", legal_entity_name)
        entity.add("previousName", previous_name)
        entity.add("alias", h.multi_split(alias, SPLITS))
        entity.add("idNumber", identifier)
        entity.add("idNumber", pass_no)
        entity.add("country", nationality_country)
        entity.add("address", h.multi_split(address, SPLITS))
        entity.add("notes", notes)
        entity.add("incorporationDate", birth_establishment_date)

        sanction = h.make_sanction(context, entity)
        sanction.add("description", sanction_type)
        sanction.add("reason", organization)
        sanction.add("program", program)  # depends on the xlsx file
        sanction.add("listingDate", listing_date)

        context.emit(entity)
        context.emit(sanction)


def parse_sheet(context: Context, sheet) -> Iterable[Dict[str, str]]:
    first_row = list(sheet.iter_rows(min_row=1, max_row=1))[0]
    headers = []
    for cell in first_row:
        normalized_header = normalize_header(str(cell.value))
        header = context.lookup_value("columns", normalized_header)
        if header is None:
            context.log.warning(
                "Unknown column title", column=normalized_header, sheet=sheet.title
            )
            header = slugify(str(cell.value))  # Use slugified header if no match found
        headers.append(header)

    for cells in sheet.iter_rows(min_row=2, values_only=True):
        yield {headers[i]: stringify(cells[i]) for i in range(len(headers))}


def crawl_xlsx(context: Context, url: str, counter: int, program: str):
    path = context.fetch_resource(f"source_{counter}.xlsx", url)
    context.export_resource(
        path,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        title=f"{context.SOURCE_TITLE} - {program}",
    )
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        context.log.info(f"Processing sheet {sheet.title} with program {program}")
        for row in parse_sheet(context, sheet):
            crawl_row(context, row, program)


def crawl_csv_row(
    context: Context, row: Dict[str, str]
):  # check for the Legal Entities in the data
    row = clean_row(row)
    full_name = row.get("full_name", "")
    if not full_name:
        return  # in the XLSX file, there are empty rows
    birth_date = row.get("date_of_birth_iso", "").strip()

    if not full_name:
        context.log.error("Missing name in row: %s", row)
        return

    person = context.make("Person")
    person.id = context.make_id(
        full_name, birth_date
    )  # Use both name and birth_date for ID
    person.add("name", full_name)
    person.add("alias", h.multi_split(row.pop("aliases", ""), NEW_LINE_SPLIT))
    person.add("birthDate", birth_date)
    person.add("nationality", row.pop("nationality", ""))
    person.add(
        "idNumber", h.multi_split(row.pop("passport_number", ""), NEW_LINE_SPLIT)
    )
    person.add(
        "idNumber",
        h.multi_split(row.pop("national_identity_number", ""), NEW_LINE_SPLIT),
    )
    person.add("address", h.multi_split(row.pop("address", ""), ADDRESS_SPLITS))
    person.add("notes", row.pop("additional_information", ""))

    context.emit(person)


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find('.//table[@class="table table-bordered"]') is not None


def crawl(context: Context):
    # Fetch the main page HTML
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=3)

    # Find the table with the relevant links
    table = doc.find('.//table[@class="table table-bordered"]')
    if table is None:
        raise ValueError("No table found in the document")

    # Find all the <a> tags within the table
    links = table.findall(".//a")
    if not links:
        raise ValueError("No links found in the table")

    # Construct the full URLs
    full_urls = [urljoin(MAIN_URL, link.get("href")) for link in links]

    found_links = []
    # Print all the parsed links
    for url in full_urls:
        section_doc = fetch_html(context, url, unblock_validator, cache_days=3)

        # Determine whether to search for .docx or .xlsx based on URL
        if "5madde_ing" in url:
            # Look for .docx link
            doc_links = section_doc.xpath('//a[contains(@href, ".docx")]')
        else:
            # Look for .xlsx link
            doc_links = section_doc.xpath('//a[contains(@href, ".xlsx")]')

            # Extract and construct full document links
        for doc_link in doc_links:
            full_doc_link = urljoin(MAIN_URL, doc_link.get("href"))
            found_links.append(full_doc_link)

    # Check if all expected links are found
    expected_links = [DOCX_LINK, *(link for link, _ in XLSX_LINK)]
    all_links_found = all(link in found_links for link in expected_links)
    if all_links_found:
        # the actual crawling part if all links are verified
        context.log.info("Fetching data from the provided XLSX links")
        for i, (url, program) in enumerate(XLSX_LINK):
            context.log.info(f"Processing URL: {url}")
            if url.endswith(".xlsx"):
                crawl_xlsx(context, url, i, program)
            else:
                raise ValueError(f"Unknown file type: {url}")
        context.log.info("Finished processing the Frozen Assets List")

        # Fetch the CSV file from the source URL
        context.log.info("Fetching data from Google Sheets CSV link")
        path = context.fetch_resource("source.csv", CSV_LINK)
        with open(path, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                crawl_csv_row(context, row)
        context.log.info("Finished processing CSV data")
    else:
        missing_links = [link for link in expected_links if link not in found_links]
        print(f"Warning: The following expected links were not found: {missing_links}")
