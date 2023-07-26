import xlrd  # type: ignore
from zavod.parse.excel import convert_excel_cell
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
