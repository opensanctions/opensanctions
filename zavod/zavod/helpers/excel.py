from typing import Dict, Generator, Optional, Union
from datetime import datetime
from normality import slugify, stringify
from xlrd.book import Book  # type: ignore
from xlrd.sheet import Cell  # type: ignore
from xlrd.xldate import xldate_as_datetime  # type: ignore
from nomenklatura.util import datetime_iso
from openpyxl.worksheet.worksheet import Worksheet

from zavod.context import Context


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


def parse_sheet(
    context: Context,
    sheet: Worksheet,
    skiprows: int = 0,
    header_lookup: Optional[str] = None,
) -> Generator[Dict[str, str | None], None, None]:
    """
    Parse an Excel sheet into a sequence of dictionaries.

    Args:
        context: Crawler context.
        sheet: The Excel sheet.
        skiprows: The number of rows to skip.
        header_lookup: The lookup key for translating headers.
    """
    headers = None
    row_counter = 0

    for row in sheet.iter_rows():
        # Increment row counter
        row_counter += 1

        # Skip the desired number of rows
        if row_counter <= skiprows:
            continue
        cells = [c.value for c in row]
        if headers is None:
            headers = []
            for idx, header in enumerate(cells):
                if header is None:
                    header = f"column_{idx}"
                if header_lookup:
                    header = context.lookup_value(
                        header_lookup,
                        stringify(header),
                        stringify(header),
                    )
                headers.append(slugify(header, sep="_"))
            continue

        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            record[header] = stringify(value)
        if len(record) == 0:
            continue
        if all(v is None for v in record.values()):
            continue
        yield record
