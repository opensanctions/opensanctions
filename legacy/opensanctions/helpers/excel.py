from typing import Optional
from xlrd.book import Book
from xlrd.sheet import Cell
from xlrd.xldate import xldate_as_datetime


def convert_excel_cell(book: Book, cell: Cell) -> Optional[str]:
    if cell.ctype == 2:
        return str(int(cell.value))
    elif cell.ctype in (0, 5, 6):
        return None
    if cell.ctype == 3:
        return xldate_as_datetime(cell.value, book.datemode)
    else:
        return cell.value
