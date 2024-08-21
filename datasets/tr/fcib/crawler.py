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
]
#     # "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/D-7262-SAYILI-KANUN-3.A-VE-3.B-MADDELERI.xlsx",


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
    name = row.get("name")
    identifier = row.get("passport_number")  # ID NUMBER
    nationality = row.get("nationality")  # NATIONALITY
    if not name:
        return  # in the C xslsx file, there are empty rows

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("idNumber", identifier)
    entity.add("country", nationality)
    # # entity.add("birthDate", row.pop("DOĞUM TARİHİ"))  # BIRTH DATE
    # # entity.add("birthPlace", row.pop("DOĞUM YERİ"))  # BIRTH PLACE
    # entity.add(
    #     "alias",
    #     h.multi_split(row.pop("KULLANDIĞI BİLİNEN DİĞER İSİMLERİ"), ALIAS_SPLITS),
    # )  # OTHER KNOWN NAMES

    # sanction = h.make_sanction(context, entity)
    # sanction.add("description", row.pop("MVD YAPTIRIM TÜRÜ"))  # SANCTION TYPE
    # sanction.add(
    #     "listingDate", row.pop("RESMİ GAZETE TARİH-SAYISI")
    # )  # OFFICIAL GAZETTE DATE

    context.emit(entity)


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
