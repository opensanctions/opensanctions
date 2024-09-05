from datetime import datetime, date
from lxml import etree
from normality import collapse_spaces, stringify, slugify
from openpyxl import load_workbook
from typing import Dict, Iterable
import re
from pantomime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html
from zavod.shed.un_sc import crawl as crawl_un_sc, Regime

DATE_FORMAT = ["%d.%m.%Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]

# original link for assertion
DOCX_LINK = "https://ms.hmb.gov.tr/uploads/sites/2/2024/08/A-BIRLESMIS-MILLETLER-GUVENLIK-KONSEYI-KARARINA-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-5.-MADDEw.docx"
XLSX_LINKS = [
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/B-YABANCI-ULKE-TALEPLERINE-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-6.-MADDE.xlsx",
        "Asset freezes pursuant to Article 6 of Law No. 6415, targeting individuals and entities based on requests made by foreign governments.",
        "B - Foreign government requests",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/09/C-IC-DONDURMA-KARARI-ILE-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-7.-MADDE-2.xlsx",
        "Asset freezes pursuant to Article 7 of Law No. 6415, targeting individuals and entities through domestic legal actions and decisions.",
        "C - Domestic legal actions",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/D-7262-SAYILI-KANUN-3.A-VE-3.B-MADDELERI.xlsx",
        "Asset freezes within the scope of Articles 3.A and 3.B of Law No. 7262, aimed at preventing the financing of proliferation of weapons of mass destruction.",
        "D - Prevention of proliferation of weapons of mass destruction",
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

NEW_LINE_SPLIT = ["\n"] + SPLITS

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

REGEX_ID_NUMBER = re.compile(r"([A-Z]{0,2}[0-9/-]{6,20})")
REGEX_GAZZETE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")


def clean_id_numbers(passport_numbers):
    return REGEX_ID_NUMBER.findall(passport_numbers)


def parse_birth_date(birth_date: str) -> str:
    """Parses the birth date and returns only the year if the date is January 1st."""
    # Check if the birth date ends with '01-01' indicating January 1st
    if birth_date.endswith("01-01"):
        # Return only the year part
        return birth_date[:4]
    return birth_date


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
    pass_no = row.pop("passport_number", "")  # Person
    identifier = row.pop("passport_number_other_info", "")  # LegalEntity
    nationality_country = row.pop("nationality_country", "")
    legal_entity_name = row.pop("legal_entity_name", "")  # LegalEntity
    birth_establishment_date = h.parse_date(
        row.pop("date_of_birth_establishment", ""), DATE_FORMAT
    )
    birth_place = row.pop("birth_place", "")  # Person
    birth_date = h.parse_date(row.pop("birth_date", ""), DATE_FORMAT)  # Person
    position = row.pop("position", "")
    address = row.pop("address", "")
    notes = row.pop("other_information", "")
    organization = row.pop("organization", "")
    sanction_type = row.pop("sanction_type", "")
    listing_date = row.pop("listing_date", "")
    gazette_date = row.pop("gazette_date", "")
    if gazette_date:
        matched_date = REGEX_GAZZETE_DATE.search(gazette_date)
        gazette_date = matched_date.group(0) if matched_date else ""

    if birth_date or birth_place:
        person = context.make("Person")
        person.id = context.make_id(name, birth_date, birth_place, pass_no, identifier)
        person.add("name", name)
        person.add("alias", h.multi_split(alias, SPLITS))
        person.add("nationality", nationality_country)
        person.add("previousName", previous_name)
        person.add("birthPlace", birth_place)
        h.apply_dates(person, "birthDate", h.multi_split(birth_date, SPLITS))
        h.apply_dates(person, "birthDate", birth_establishment_date)
        person.add("passportNumber", collapse_spaces(pass_no))
        person.add("position", position)
        person.add("address", h.multi_split(address, SPLITS))
        person.add("notes", notes)
        person.add("topics", "sanction")

        sanction = h.make_sanction(context, person)
        sanction.add("description", sanction_type)
        sanction.add("reason", organization)
        sanction.add("program", program)  # depends on the xlsx file
        h.apply_date(sanction, "listingDate", listing_date)
        h.apply_date(sanction, "listingDate", gazette_date)

        context.emit(person, target=True)
        context.emit(sanction)
    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, birth_date, birth_place, pass_no, identifier)
        entity.add("name", name)
        entity.add("name", legal_entity_name)
        entity.add("previousName", previous_name)
        entity.add("alias", h.multi_split(alias, SPLITS))
        entity.add("idNumber", collapse_spaces(identifier))
        entity.add("idNumber", collapse_spaces(pass_no))
        entity.add("country", nationality_country)
        entity.add("address", h.multi_split(address, SPLITS))
        entity.add("notes", notes)
        entity.add("incorporationDate", birth_establishment_date)
        entity.add("topics", "sanction")

        sanction = h.make_sanction(context, entity)
        sanction.add("description", sanction_type)
        sanction.add("reason", organization)
        sanction.add("program", program)  # depends on the xlsx file
        h.apply_date(sanction, "listingDate", listing_date)
        h.apply_date(sanction, "listingDate", gazette_date)

        context.emit(entity, target=True)
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


def crawl_xlsx(context: Context, url: str, program: str, short_name: str):
    path = context.fetch_resource(f"{short_name}.xlsx", url)
    context.export_resource(
        path,
        XLSX,
        title=f"{context.SOURCE_TITLE} - {short_name}",
    )
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        context.log.info(f"Processing sheet {sheet.title} with program {program}")
        for row in parse_sheet(context, sheet):
            crawl_row(context, row, program)


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find('.//table[@class="table table-bordered"]') is not None


def crawl(context: Context):
    # Fetch the main page HTML
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=3)
    doc.make_links_absolute(context.data_url)

    # Find the table with the relevant links
    table = doc.find('.//table[@class="table table-bordered"]')

    # Find all the <a> tags within the table
    links = table.findall(".//a")

    # Construct the full URLs
    full_urls = [link.get("href") for link in links]

    found_links = set()
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
            found_links.add(doc_link.get("href"))

    # Check if all expected links are found
    expected_links = set([DOCX_LINK, *(link for link, _, _ in XLSX_LINKS)])
    new_links = found_links - expected_links
    assert not new_links, f"Unexpected links found: {new_links}"

    missing_links = expected_links - found_links
    assert not missing_links, f"Expected links not found: {missing_links}"

    # the actual crawling part if all links are verified
    context.log.info("Fetching data from the provided XLSX links")
    for url, program, short in XLSX_LINKS:
        context.log.info(f"Processing URL: {url}")
        crawl_xlsx(context, url, program, short)
    context.log.info("Finished processing the Excel files")

    # Publish UN Security Council sanctions
    crawl_un_sc(context, [Regime.TALIBAN, Regime.DAESH_AL_QAIDA])
