import xlrd  # type: ignore
from zavod.helpers.excel import convert_excel_cell, convert_excel_date
from zavod.tests.conftest import FIXTURES_PATH

XLS_BOOK = FIXTURES_PATH / "book.xls"


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
