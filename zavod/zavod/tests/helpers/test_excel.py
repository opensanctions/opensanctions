import xlrd  # type: ignore
from openpyxl import load_workbook
from zavod.context import Context
from zavod.helpers.excel import convert_excel_cell, convert_excel_date, parse_sheet
from zavod.tests.conftest import FIXTURES_PATH

XLS_BOOK = FIXTURES_PATH / "book.xls"
XLSX_BOOK = FIXTURES_PATH / "book.xlsx"


def test_excel_cell():
    book = xlrd.open_workbook(XLS_BOOK.as_posix())
    for sheet in book.sheets():
        row = sheet.row(0)
        cells = [convert_excel_cell(book, cell) for cell in row]
        assert cells[0] == "numeric"
        row = sheet.row(1)
        cells = [convert_excel_cell(book, cell) for cell in row]
        assert cells[0] == "1"
        assert cells[1] == "2023-07-26T00:00:00"


def test_excel_date():
    assert convert_excel_date(44876) == "2022-11-11T00:00:00"
    assert convert_excel_date(44876.0) == "2022-11-11T00:00:00"
    assert convert_excel_date("44876") == "2022-11-11T00:00:00"
    assert convert_excel_date("44876.0") == "2022-11-11T00:00:00"
    assert convert_excel_date("foo") is None
    assert convert_excel_date(0) is None
    assert convert_excel_date(1) is None
    assert convert_excel_date(3_999) is None
    assert convert_excel_date(100_001) is None
    assert convert_excel_date(None) is None


def test_parse_sheet(vcontext: Context):
    book = load_workbook(XLSX_BOOK.as_posix())
    sheet = book.active
    rows = list(parse_sheet(vcontext, sheet))
    assert len(rows) == 1
    assert rows[0] == {
        "numeric": "1",
        "text": "Hello, World!",
        "date": "2023-07-26",
        "column_3": None,
        "column_4": None,
    }
