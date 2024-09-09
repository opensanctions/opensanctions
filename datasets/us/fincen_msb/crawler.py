import xlrd
from datetime import datetime
from typing import Dict, List
from rigour.mime.types import XLS
from normality import collapse_spaces, stringify
from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, sheet: str, section: str, row: Dict[str, List[str]]):
    entity = context.make("LegalEntity")
    legal_name = row.pop("LEGAL NAME")
    entity.id = context.make_id(legal_name)
    entity.add("name", legal_name)
    context.emit(entity, target=True)


def crawl_xls(context: Context, path: str):
    # Open the Excel workbook and iterate through sheets and rows
    xls = xlrd.open_workbook(path)
    for sheet in xls.sheets():
        headers = None
        for r in range(sheet.nrows):
            row = [h.convert_excel_cell(xls, c) for c in sheet.row(r)]
            if headers is not None:
                data: Dict[str, List[str]] = {}
                for header, cell in zip(headers, row):
                    if header is None:
                        continue
                    values = []
                    if isinstance(cell, datetime):
                        cell = cell.date()
                    for value in stringify(cell):
                        if value is None:
                            continue
                        values.append(value)
                    data[header] = values
                crawl_row(context, sheet.name, "msb", data)
            else:
                headers = [collapse_spaces(cell) for cell in row]


def crawl(context: Context):
    # Perform the POST request with headers
    response = context.http.post(context.data_url, data={"site": "AA"}, stream=True)

    # Debug: Print the Content-Type to ensure we're getting the correct file
    content_type = response.headers.get("Content-Type")
    print(f"Content-Type: {content_type}")

    # Debug: Print the first few characters of the HTML content for troubleshooting
    content_preview = response.raw.read(100)
    print(f"Received content preview: {content_preview.decode(errors='ignore')}")

    # Save the Excel file and process it
    path = context.get_resource_path("source.xls")
    with open(path, "wb") as file:
        file.write(response.content)

    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    crawl_xls(context, path)
