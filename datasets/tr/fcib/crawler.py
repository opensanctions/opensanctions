from datetime import datetime, date
from openpyxl import load_workbook
from typing import Dict
from normality import collapse_spaces, stringify, normalize
from zavod import Context
from zavod import helpers as h

# Constants
XLSX_LINK = [
    "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/B-YABANCI-ULKE-TALEPLERINE-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-6.-MADDE.xlsx",
    "https://ms.hmb.gov.tr/uploads/sites/2/2024/06/C-6415-SAYILI-KANUN-7.-MADDE.xlsx",
    "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/D-7262-SAYILI-KANUN-3.A-VE-3.B-MADDELERI.xlsx",
]


ALIAS_SPLITS = [
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


def crawl_row(context: Context, row: Dict[str, str]):
    # name = row.pop("ADI SOYADI-ÜNVANI")  # NAME-SURNAME-TITLE
    name = row.pop("name")
    if not name:
        return  # in the C xslsx file, there are empty rows
    alias = row.pop("alias")
    previous_name = row.pop("previous_name", "")
    internal_id = row.pop("sequence_no")
    pass_no = row.pop("passport_number")  # Person
    nationality = row.pop("nationality")

    legal_entity_name = row.pop("legal_entity_name", "")  # LegalEntity
    birth_establishment_date = row.pop("date_of_birth_establishment", "")  # LegalEntity
    birth_place = row.pop("birth_place", "")  # Person
    birth_date = row.pop("birth_date", "")  # Person
    # position = row.pop("position", "")
    # address = row.pop("address")
    # notes = row.pop("other_information")
    # organization = row.pop("organization")

    sanction_type = row.pop("sanction_type", "")
    listing_date = row.pop("listing_date", "")
    # official_gazette_date = row.pop("official_gazette_date")

    if birth_date or birth_place or pass_no or nationality:
        person = context.make("Person")
        person.id = context.make_id(name, internal_id)
        person.add("name", name)
        person.add(
            "alias",
            h.multi_split(alias, ALIAS_SPLITS),
        )
        person.add("nationality", nationality)
        person.add("previousName", previous_name)
        context.emit(person)

    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name)
        entity.add("name", name)
        entity.add("idNumber", pass_no)
        entity.add("country", nationality)
        # entity.add("birthDate", row.pop("birth_date"))  # BIRTH DATE
        # entity.add("birthPlace", row.pop("birth_place"))  # BIRTH PLACE
        entity.add(
            "alias",
            h.multi_split(alias, ALIAS_SPLITS),
        )

        sanction = h.make_sanction(context, entity)
        sanction.add("description", sanction_type)
        sanction.add("listingDate", listing_date)  # OFFICIAL GAZETTE DATE

        context.emit(entity)

    entity_name = row.get("legal_entity_name")
    if entity_name:
        legal_entity = context.make("LegalEntity")
        legal_entity.id = context.make_id(entity_name)
        legal_entity.add("name", entity_name)
        context.emit(legal_entity)


def process_sheet(context: Context, sheet):
    # First, normalize and map the headers using lookup
    first_row = list(sheet.iter_rows(min_row=1, max_row=1))[0]
    headers = []

    for cell in first_row:
        normalized_header = normalize_header(str(cell.value))
        header = context.lookup_value("columns", normalized_header)
        if header is None:
            context.log.warning(
                "Unknown column title", column=normalized_header, sheet=sheet.title
            )
            header = normalized_header  # Use normalized header if no match found
        headers.append(header)

    # Process each subsequent row in the sheet
    for cells in sheet.iter_rows(min_row=2, values_only=True):
        row = {headers[i]: stringify(cells[i]) for i in range(len(headers))}
        crawl_row(context, row)


def crawl_xlsx(context: Context, url: str, counter: int):
    # Create a unique filename for each resource
    path = context.fetch_resource(f"source_{counter}.xlsx", url)
    context.export_resource(
        path,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        title=context.SOURCE_TITLE,
    )
    wb = load_workbook(path, read_only=True)

    for sheet in wb.worksheets:
        process_sheet(context, sheet)


def crawl(context: Context):
    context.log.info("Fetching data from the provided XLSX links")

    # Iterate over each link in the XLSX_LINK list with a counter
    for i, url in enumerate(XLSX_LINK):
        context.log.info(f"Processing URL: {url}")
        # Process the XLSX file with a unique counter
        if url.endswith(".xlsx"):
            crawl_xlsx(context, url, i)
        else:
            raise ValueError(f"Unknown file type: {url}")

    context.log.info("Finished processing the Frozen Assets List")
