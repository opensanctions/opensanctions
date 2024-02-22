from typing import Optional, Union
from datetime import datetime
from xlrd.book import Book  # type: ignore
from xlrd.sheet import Cell  # type: ignore
from xlrd.xldate import xldate_as_datetime  # type: ignore
from nomenklatura.util import datetime_iso


def convert_excel_cell(book: Book, cell: Cell) -> Optional[str]:
    """Convert an Excel cell to a string, handling different types.

    Args:
        book: The Excel workbook.
        cell: The Excel cell.

    Returns:
        The cell value as a string, or `None` if the cell is empty.
    """
    if cell.ctype == 2:
        return str(int(cell.value))
    elif cell.ctype in (0, 5, 6):
        return None
    if cell.ctype == 3:
        dt: datetime = xldate_as_datetime(cell.value, book.datemode)
        return datetime_iso(dt)
    else:
        if cell.value is None:
            return None
        return str(cell.value)


def convert_excel_date(value: Optional[Union[str, int, float]]) -> Optional[str]:
    """Convert an Excel date to a string.

    Args:
        value: The Excel date value (e.g. 44876).

    Returns:
        The date value as a string, or `None` if the value is empty.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return None
    if isinstance(value, float):
        value = int(value)
    if value < 4_000 or value > 100_000:
        return None
    dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + value - 2)
    return datetime_iso(dt)
