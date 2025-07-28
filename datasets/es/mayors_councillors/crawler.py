from rigour.mime.types import XLSX
from openpyxl import load_workbook
from typing import Dict

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

IGNORE = [
    "column_0",
    "column_1",
    "column_2",
    "autonomous_community",
    "ine_code",
    "party",
    "column_11",
    "column_12",
    "column_13",
    "column_14",
    "column_15",
    "column_16",
    "column_17",
]


def crawl_item(context: Context, row: Dict[str, str]):
    name = row.pop("name")
    province = row.pop("province")
    municipality = row.pop("municipality")
    first_last_name = row.pop("last_name", None)
    second_last_name = row.pop("second_last_name", None)
    position = row.pop("position", None)

    if not name or not province or not municipality:
        context.log.warning("Missing required fields", row=row)
        return

    pep = context.make("Person")
    pep.id = context.make_id(name, province, municipality)
    if first_last_name or second_last_name:
        last_name = f"{first_last_name} {second_last_name}"
        h.apply_name(pep, first_name=name, last_name=last_name)
    else:
        pep.add("name", name)
    position = h.make_position(
        context,
        # Positions are available for the current officials, historical data lists only mayors
        position if position else "Mayor",
        country="es",
        subnational_area=f"{municipality}, {province}",
        # organization=row.pop("party"), # Name of the political party
        lang="spa",
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=row.pop("start_date"),
        end_date=row.pop("end_date", None),
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(occupancy)
        context.emit(pep)

    context.audit_data(row, IGNORE)


def crawl(context: Context):
    hist_doc = context.fetch_html(context.dataset.model.url, cache_days=1)
    hist_doc.make_links_absolute(context.dataset.model.url)
    hist_url = hist_doc.xpath(
        ".//div[@class='com-listado com-listado--destacado']//a[contains(@href, 'Alcaldes_Mandato_2019_2023')]/@href"
    )
    assert len(hist_url) == 1, "Expected exactly one historical URL"
    path = context.fetch_resource("historical.xlsx", hist_url[0])
    context.export_resource(path, XLSX, title="Mayors 2019-2023")
    wb = load_workbook(path, read_only=True)
    if len(wb.sheetnames) != 1:
        raise Exception("Expected only one sheet in the workbook")

    for row in h.parse_xlsx_sheet(
        context, wb[wb.sheetnames[0]], skiprows=7, header_lookup="columns"
    ):
        crawl_item(context, row)

    path = context.fetch_resource("mayors_councillors.xlsx", "https://concejales.redsara.es/consulta/getConcejalesLegislatura")
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = load_workbook(path, read_only=True)
    if len(workbook.sheetnames) != 1:
        raise Exception("Expected only one sheet in the workbook")

    for row in h.parse_xlsx_sheet(context,  workbook[workbook.sheetnames[0]], skiprows=5, header_lookup="columns"):
        crawl_item(context, row)
