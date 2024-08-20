from datetime import datetime, date
from openpyxl import load_workbook
from typing import Dict
from normality import collapse_spaces, stringify
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

# Extended COLUMN_NAMES_MAP to include all possible variations
COLUMN_NAMES_MAP = {
    "Gerçek Kişi Soyadı Ünvanı": "full_name",
    "Eski Adı": "former_name",
    "Tüzel Kuruluş/Organizasyon Ünvanı": "organization_name",
    "Kullandığı Bilinen Diğer İsmler": "other_known_names",
    "Pasaport No/ Diğer Muhtelif Bilgiler": "passport_other_info",
    "Görevi": "position",
    "Adres": "address",
    "Uyruğu": "nationality",
    "Listeye Alınma Tarihi": "date_listed",
    "Diğer Bilgiler": "other_information",
    "Anne Adı": "mother_name",
    "Baba Adı": "father_name",
    "Doğum Tarihi": "birth_date",
    "Örgütü": "organization",
    "R.Gazete Tarih Sayı": "official_gazette_date_number",
    "BKK-CBK Karar Tarih ve Sayısı": "decision_date_number",
    "ADI SOYADI-ÜNVANI": "full_name",
    "TCKN-VKN-PASAPORT NO": "passport_other_info",
    "UYRUĞU": "nationality",
    "MVD YAPTIRIM TÜRÜ": "other_information",
    "DOĞUM TARİHİ": "birth_date",
    "DOĞUM YERİ": "birth_place",
    "RESMİ GAZETE TARİH-SAYISI": "official_gazette_date_number",
    "KULLANDIĞI BİLİNEN DİĞER İSİMLERİ": "other_known_names",
    "TABİ OLDUĞU DİĞER UYRUKLAR": "other_nationalities",
    "OYU DİĞER UYRUKLAR": "other_nationalities",
    "KARAR TARİH-SAYISI": "decision_date_number",
    "RESMİ GAZETE TARİH SAYISI": "official_gazette_date_number",
    "Doğum Tarihi/Kuruluş": "birth_date",
    "Doğum Yeri": "birth_place",
    "GERÇEK/TÜZEL KİŞİ/KURULUŞ/ORGANİZASYON ADI SOYADI ÜNVANI": "full_name",
}


def str_cell(cell: object) -> str:
    """Convert a cell value to string, handling dates."""
    if cell is None:
        return ""
    if isinstance(cell, (datetime, date)):
        return cell.isoformat()
    return str(cell)


def crawl_row(context: Context, row: Dict[str, str]):
    # name = row.pop("ADI SOYADI-ÜNVANI")  # NAME-SURNAME-TITLE
    name = row.pop("full_name")
    identifier = row.pop("passport_other_info", "")  # ID NUMBER
    nationality = row.pop("nationality", "")  # NATIONALITY

    entity = context.make("LegalEntity")
    entity.id = context.make_id(identifier, name)
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
    headers = [
        collapse_spaces(str(c.value))
        for c in list(sheet.iter_rows(min_row=1, max_row=1))[0]
    ]

    # Translate headers to standardized English names
    standardized_headers = [COLUMN_NAMES_MAP.get(header, header) for header in headers]

    for cells in sheet.iter_rows(min_row=2, values_only=True):
        row = {
            standardized_headers[i]: stringify(cells[i])
            for i in range(len(standardized_headers))
        }
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
