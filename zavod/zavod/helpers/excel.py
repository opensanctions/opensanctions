from typing import Dict, Generator, Iterator, List, Optional, Union
from datetime import datetime
from datapatch import Lookup
from normality import slugify_text, stringify
from xlrd import (
    XL_CELL_DATE,
    XL_CELL_EMPTY,
    XL_CELL_ERROR,
    XL_CELL_BLANK,
    XL_CELL_NUMBER,
)
from xlrd.book import Book
from xlrd.sheet import Cell, Sheet
from xlrd.xldate import xldate_as_datetime
from rigour.time import datetime_iso
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
    # https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell
    if cell.ctype == XL_CELL_NUMBER:
        return str(int(cell.value))
    elif cell.ctype in (XL_CELL_EMPTY, XL_CELL_ERROR, XL_CELL_BLANK):
        return None
    if cell.ctype == XL_CELL_DATE:
        assert isinstance(cell.value, float)
        dt = xldate_as_datetime(cell.value, book.datemode)
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


def parse_xls_sheet(
    context: Context,
    sheet: Sheet,
    skiprows: int = 0,
    join_header_rows: int = 0,
) -> Generator[Dict[str, str | None], None, None]:
    """
    Parse an Excel sheet into a sequence of dictionaries.

    Keys are the column headings slugified with _ as separator.

    Cells with links are included as keys with _url appended to the original key.
    """
    headers: List[str] | None = None
    for row_ix, row in enumerate(sheet):
        if row_ix < skiprows:
            continue
        cells: List[Optional[str]] = []
        record: Dict[str, str | None] = {}
        for cell_ix, xl_cell in enumerate(row):
            if xl_cell.ctype == XL_CELL_DATE:
                # Convert Excel date format to zavod date
                assert isinstance(xl_cell.value, float)
                assert sheet.book is not None
                date_value = xldate_as_datetime(xl_cell.value, sheet.book.datemode)
                cells.append(date_value.date().isoformat())
            else:
                cells.append(stringify(xl_cell.value))

            # Add link to key ..._url
            if url := sheet.hyperlink_map.get((row_ix, cell_ix)):
                assert headers is not None, ("URLs not supported in headers yet.", row)
                key = f"{headers[cell_ix]}_url"
                record[key] = str(url.url_or_path)

        if headers is None or join_header_rows > 0:
            if headers:
                # Append row of split-headers to current headers
                for col_idx, cell in enumerate(cells):
                    if not cell:
                        continue
                    headers[col_idx] += f"_{slugify_text(cell, sep='_')}"
                join_header_rows -= 1
            else:
                # Initialise first row of headers
                headers = []
                for idx, cell in enumerate(cells):
                    if not cell:
                        cell = f"column_{idx}"
                    headers.append(slugify_text(cell, "_") or "")
            continue

        for header, value in zip(headers, cells):
            record[header] = stringify(value)

        if len(record) == 0:
            continue
        if all(v is None for v in record.values()):
            continue
        yield record


def parse_xlsx_sheet(
    context: Context,
    sheet: Worksheet,
    skiprows: int = 0,
    header_lookup: Optional[Lookup] = None,
    extract_links: bool = False,
) -> Iterator[Dict[str, str | None]]:
    """
    Parse an Excel sheet into a sequence of dictionaries.

    Args:
        context: Crawler context.
        sheet: The Excel sheet.
        skiprows: The number of rows to skip.
        header_lookup: The lookup key for translating headers.
        extract_links: Whether to extract hyperlinks. Only works when read_only=False
    """
    headers: Optional[List[str]] = None
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
                header = stringify(header)
                if header is None:
                    header = f"column_{idx}"
                if header_lookup:
                    header = header_lookup.get_value(header) or header
                header_slug = slugify_text(header, sep="_")
                if header_slug is None and header is not None:
                    header_slug = f"column_{idx}"
                headers.append(header_slug)
            continue

        record: dict[str, str | None] = {}
        for cell_ix, (header, cell) in enumerate(zip(headers, row)):
            value = cell.value
            if isinstance(value, datetime):
                value = value.date()
            record[header or ""] = stringify(value)

            if extract_links:
                # Check if the cell has a hyperlink
                if cell.hyperlink:
                    key = f"{header}_url"
                    record[key] = str(cell.hyperlink.target)

        if len(record) == 0:
            continue
        if all(v is None for v in record.values()):
            continue
        for header in headers:
            if header not in record:
                record[header] = None
        yield record
