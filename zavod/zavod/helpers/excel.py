from collections.abc import Generator, Iterator, Sequence
from datetime import datetime
from typing import Any
from datapatch import Lookup
from normality import slugify_text, stringify
from rigour.time import datetime_iso
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
from openpyxl.worksheet.worksheet import Worksheet
from python_calamine import CalamineSheet

from zavod.context import Context


def convert_excel_cell(book: Book, cell: Cell) -> str | None:
    """Convert an Excel cell to a string, handling different types.

    Args:
        book: The Excel workbook.
        cell: The Excel cell.

    Returns:
        The cell value as a string, or `None` if the cell is empty.
    """
    # https://xlrd.readthedocs.io/en/latest/api.html#xlrd.sheet.Cell
    if cell.ctype == XL_CELL_NUMBER:
        # Excel stores all numbers as floats; stringify keeps the fractional part.
        return stringify(cell.value)
    elif cell.ctype in (XL_CELL_EMPTY, XL_CELL_ERROR, XL_CELL_BLANK):
        return None
    if cell.ctype == XL_CELL_DATE:
        assert isinstance(cell.value, float)
        dt = xldate_as_datetime(cell.value, book.datemode)
        # e.g. "2023-07-26T00:00:00" — Excel cells carry no timezone.
        return datetime_iso(dt)
    else:
        if cell.value is None:
            return None
        return str(cell.value)


def convert_excel_date(value: str | int | float | None) -> str | None:
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
    # e.g. "2022-11-11T00:00:00" — Excel dates carry no timezone.
    return datetime_iso(dt)


def parse_xls_sheet(
    context: Context,
    sheet: Sheet,
    skiprows: int = 0,
    join_header_rows: int = 0,
) -> Generator[dict[str, str | None], None, None]:
    """
    Parse an Excel sheet into a sequence of dictionaries.

    Keys are the column headings slugified with _ as separator.

    Cells with links are included as keys with _url appended to the original key.
    """
    headers: list[str] | None = None
    headers_validated = False
    for row_ix, row in enumerate(sheet):
        if row_ix < skiprows:
            continue
        cells: list[str | None] = []
        record: dict[str, str | None] = {}
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

        if not headers_validated:
            # Headers are final once the first data row is reached (they may
            # span several rows via join_header_rows). Records are built by
            # zipping headers with cells, so a duplicate header would silently
            # drop the earlier column's cell.
            duplicates = {hdr for hdr in headers if headers.count(hdr) > 1}
            assert not duplicates, f"Duplicate headers: {sorted(duplicates)}"
            headers_validated = True

        for header, value in zip(headers, cells):
            record[header] = stringify(value)

        if len(record) == 0:
            continue
        if all(v is None for v in record.values()):
            continue
        yield record


def _xlsx_headers(cells: Sequence[Any], header_lookup: Lookup | None) -> list[str]:
    """
    Convert the cells of a header row into record keys.

    Both XLSX parsers use this function, so that they make the same keys.
    An empty cell, or a heading that slugifies to nothing, gets the key
    `column_<index>`. The lookup translates the heading before it is slugified.

    Args:
        cells: The cell values of the header row.
        header_lookup: The lookup for translating headings.

    Raises:
        AssertionError: If two columns get the same key.
    """
    headers: list[str] = []
    for idx, cell in enumerate(cells):
        header = stringify(cell)
        if header is None:
            header = f"column_{idx}"
        if header_lookup:
            header = header_lookup.get_value(header) or header
        headers.append(slugify_text(header, sep="_") or f"column_{idx}")
    _assert_unique_headers(headers)
    return headers


def _assert_unique_headers(headers: Sequence[str]) -> None:
    """
    Check that no two columns get the same record key.

    Records are built by zipping headers with cells, so a duplicate header
    would silently drop the earlier column's cell.

    Raises:
        AssertionError: If two columns get the same key.
    """
    duplicates = {hdr for hdr in headers if headers.count(hdr) > 1}
    assert not duplicates, f"Duplicate headers: {sorted(duplicates)}"


def parse_xlsx_sheet(
    context: Context,
    sheet: CalamineSheet,
    skiprows: int = 0,
    header_lookup: Lookup | None = None,
    headers: Sequence[str] | None = None,
) -> Iterator[dict[str, str | None]]:
    """
    Parse an Excel sheet read with python-calamine into a sequence of dictionaries.

    Keys are the column headings slugified with _ as separator. Columns without
    a heading get the key `column_<index>`. Date and datetime cells become ISO
    dates, numbers drop a zero fractional part (`2.0` gives `"2"`), and empty
    cells become `None`.

    Use `parse_xlsx_sheet_openpyxl` if you need hyperlinks from the cells.

    Args:
        context: Crawler context.
        sheet: The sheet, e.g. from `CalamineWorkbook.from_path(path).get_sheet_by_name(name)`.
        skiprows: The number of rows to skip, counted from the first row of the sheet.
        header_lookup: The lookup key for translating headers.
        headers: The record keys in column order, used as given. Every row
            after `skiprows` is then a data row, so skip the heading row too.
            Use this when the headings of the sheet cannot give unique keys,
            e.g. two columns with the same heading. The crawler should then
            check the heading row itself, so that a change in the columns
            of the source makes it fail.

    Raises:
        ValueError: If both `headers` and `header_lookup` are given, or if a
            row has a value in a column beyond `headers`.
        AssertionError: If two columns get the same key.
    """
    if headers is not None and header_lookup is not None:
        raise ValueError("Give either headers or header_lookup, not both.")
    keys: list[str] | None = None
    if headers is not None:
        keys = list(headers)
        _assert_unique_headers(keys)

    # Keep leading empty rows and columns so that skiprows and column_<index>
    # keys count from cell A1.
    rows = sheet.to_python(skip_empty_area=False)
    for row_ix, row in enumerate(rows[skiprows:], start=skiprows):
        if keys is None:
            keys = _xlsx_headers(row, header_lookup)
            continue

        # Records are built by zipping keys with cells, which drops the cells
        # of columns beyond the keys.
        dropped = [v for v in row[len(keys) :] if stringify(v) is not None]
        if dropped:
            raise ValueError(
                f"Row {row_ix + 1} has values beyond the {len(keys)} headers: {dropped}"
            )

        record: dict[str, str | None] = {key: None for key in keys}
        for key, value in zip(keys, row):
            if isinstance(value, datetime):
                value = value.date()
            record[key] = stringify(value)

        if all(v is None for v in record.values()):
            continue
        yield record


def parse_xlsx_sheet_openpyxl(
    context: Context,
    sheet: Worksheet,
    skiprows: int = 0,
    header_lookup: Lookup | None = None,
    extract_links: bool = False,
) -> Iterator[dict[str, str | None]]:
    """
    Parse an Excel sheet into a sequence of dictionaries.

    Args:
        context: Crawler context.
        sheet: The Excel sheet.
        skiprows: The number of rows to skip.
        header_lookup: The lookup key for translating headers.
        extract_links: Whether to extract hyperlinks. Only works when read_only=False
    """
    headers: list[str] | None = None
    row_counter = 0

    for row in sheet.iter_rows():
        # Increment row counter
        row_counter += 1

        # Skip the desired number of rows
        if row_counter <= skiprows:
            continue
        if headers is None:
            headers = _xlsx_headers([c.value for c in row], header_lookup)
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
