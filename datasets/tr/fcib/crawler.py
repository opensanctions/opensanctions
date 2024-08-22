from datetime import datetime, date
from normality import collapse_spaces, stringify, slugify
from openpyxl import load_workbook
from typing import Dict, Iterable
import requests
import hashlib
from lxml import html
import csv
from typing import List, Any

from zavod import Context
from zavod import helpers as h

DATE_FORMAT = "%d.%m.%Y"

CSV_LINK = "https://docs.google.com/spreadsheets/d/1SFH2gKt2gFVCNvl2wnNuFZT3m-iVNYlXiWHXRquddFI/pub?gid=594686664&single=true&output=csv"


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


def clean_row(row: Dict[str, str]) -> Dict[str, str]:
    """Clean non-standard spaces from row keys and values."""
    return {
        k.replace("\xa0", " ").strip(): v.replace("\xa0", " ").strip()
        for k, v in row.items()
    }


# Helper function to fetch and parse HTML
# def fetch_html(url):
#     response = requests.get(url)
#     response.raise_for_status()
#     return html.fromstring(response.content)


# Helper function to hash file content
def hash_file_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return hashlib.sha1(response.content).hexdigest()


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


# def crawl_csv_row(context: Context, row: Dict[str, str]):
#     row = clean_row(row)
#     full_name = row.get("full_name", "")
#     if not full_name:
#         return  # in the XLSX file, there are empty rows
#     birth_date = row.get("date_of_birth_iso", "").strip()

#     if not full_name:
#         context.log.error("Missing name in row: %s", row)
#         return

#     person_unsc = context.make("Person")
#     person_unsc.id = context.make_id(
#         full_name, birth_date
#     )  # Use both name and birth_date for ID
#     person_unsc.add("name", full_name)
#     person_unsc.add("alias", row.pop("aliases", ""))
#     person_unsc.add("birthDate", birth_date)
#     person_unsc.add("nationality", row.pop("nationality", ""))
#     person_unsc.add("idNumber", row.pop("passport_number", ""))
#     person_unsc.add("idNumber", row.pop("national_identity_number", ""))
#     person_unsc.add("address", row.pop("address", ""))
#     person_unsc.add("notes", row.pop("additional_information", ""))

#     context.emit(person_unsc)
#     context.log.info(f"Emitted entity for person: {full_name}")


# def crawl(context: Context):
#     context.log.info("Fetching data from the provided XLSX links")
#     for i, (url, program) in enumerate(XLSX_LINK):
#         context.log.info(f"Processing URL: {url}")
#         context.log.info(f"Program description: {program}")
#         if url.endswith(".xlsx"):
#             crawl_xlsx(context, url, i, program)
#         else:
#             raise ValueError(f"Unknown file type: {url}")
#     context.log.info("Finished processing the Frozen Assets List")

#     # # Fetch the CSV file from the source URL
#     # context.log.info("Fetching data from Google Sheets CSV link")
#     # path = context.fetch_resource("source.csv", CSV_LINK)
#     # with open(path, "r", encoding="latin-1") as fh:
#     #     reader = csv.DictReader(fh)
#     #     print(reader.fieldnames)
#     #     for row in reader:
#     #         print(row)
#     #         crawl_csv_row(context, row)

#     # context.log.info("Finished processing CSV data")


def crawl(context: Context):
    # Fetch the main page HTML
    doc = context.fetch_html(context.data_url)
    print(html.tostring(doc, pretty_print=True, encoding="unicode"))
    # Find the div with the relevant links
    container = doc.xpath(
        '//div[@class="col-xl-9 col-12 order-xl-last page-content-inner"]'
    )
    if not container:
        raise ValueError("No container found in the document")

    # Find the table with the relevant links
    tables = doc.findall(
        '//div[@class="col-xl-3 col-12 order-xl-first order-last page-sidebar"]'
    )
    if not tables:
        raise ValueError("No table found in the document")

    # Process each table element found
    for table in tables:
        links = table.xpath(".//a")
        # Extract partial links and navigate to each section
        for link in links:
            partial_link = link.get("href")
            section_url = f"https://en.hmb.gov.tr{partial_link}"
            section_doc = context.fetch_html(section_url)
            # Find the document link
            doc_links = section_doc.xpath(
                '//a[contains(@href, ".xlsx") or contains(@href, ".docx")]'
            )
            for doc_link in doc_links:
                full_doc_link = f"https://ms.hmb.gov.tr{doc_link.get('href')}"
                # Check if the link is an XLSX link and compare it with the predefined list
                if full_doc_link.endswith(".xlsx"):
                    matched = False
                    for url, program in XLSX_LINK:
                        if url == full_doc_link:
                            matched = True
                            print(f"Found matching XLSX link: {full_doc_link}")
                            break
                    if not matched:
                        print(f"Unexpected XLSX link found: {full_doc_link}")
                # If the link is a DOCX link, compute its hash
                elif full_doc_link.endswith(".docx"):
                    doc_hash = hash_file_content(full_doc_link)
                    print(f"DOCX link hash: {doc_hash}")
