import xlrd  # type: ignore
from openpyxl import load_workbook
from zavod.context import Context
from zavod.helpers.excel import (
    convert_excel_cell,
    convert_excel_date,
    parse_xlsx_sheet,
    parse_xls_sheet,
)
from zavod.tests.conftest import FIXTURES_PATH

XLS_BOOK = FIXTURES_PATH / "book.xls"
# Fields with blank-fallback headings created in Google Sheets by havinga a value
# in right-most most column (with blank header).
XLSX_BOOK = FIXTURES_PATH / "book.xlsx"


def test_excel_cell():
    book = xlrd.open_workbook(XLS_BOOK.as_posix())
    sheet = book.sheet_by_name("basic")
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


def test_parse_xls_sheet(vcontext: Context):
    book = xlrd.open_workbook(XLS_BOOK.as_posix())
    sheet = book.sheet_by_name("basic")
    rows = list(parse_xls_sheet(vcontext, sheet))
    assert len(rows) == 1, rows
    assert rows[0] == {
        "numeric": "1",
        "text": "Hello, World!",
        "date": "2023-07-26",
        "numeric_url": "http://example.com/1",
        "text_url": "http://example.com/hello",
    }


def test_parse_xls_sheet_split_header(vcontext: Context):
    book = xlrd.open_workbook(XLS_BOOK.as_posix())
    sheet = book.sheet_by_name("split headers")
    rows = list(parse_xls_sheet(vcontext, sheet, skiprows=1, join_header_rows=1))
    assert len(rows) == 1, rows
    assert rows[0] == {
        "a": "aa",
        "thing_b": "bb",
        "thing_c": "cc",
    }


def test_parse_xlsx_sheet(vcontext: Context):
    book = load_workbook(XLSX_BOOK.as_posix())
    sheet = book.active
    rows = list(parse_xlsx_sheet(vcontext, sheet, extract_links=True))
    assert len(rows) == 1
    assert rows[0] == {
        "column_0": None,
        "numeric": "1",
        "text": "Hello, World!",
        "date": "2023-07-26",
        "numeric_url": "http://example.com/1",
        "text_url": "http://example.com/hello",
        "column_4": "blank_header_value",
    }
