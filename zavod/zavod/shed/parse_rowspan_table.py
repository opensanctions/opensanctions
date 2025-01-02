from lxml.html import HtmlElement
from typing import Dict, Generator

from zavod import Context


def parse_table_with_rowspan(
    context: Context,
    table: HtmlElement,
) -> Generator[Dict[str, HtmlElement], None, None]:
    headers = None
    rowspan_cells = {}  # Dictionary to keep track of cells with rowspan

    for row_index, row in enumerate(table.findall(".//tr")):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                eltree = el  # If necessary, you can use cast(HtmlElement, el)
                label = eltree.text_content().strip()
                headers.append(context.lookup_value("headers", label))
            continue

        cells = row.findall("./td")
        row_data = {}

        cell_index = 0  # To track which header corresponds to which cell
        for idx, cell in enumerate(cells):
            while cell_index in rowspan_cells and rowspan_cells[cell_index][1] > 0:
                # If we have data from a rowspan to apply, take it
                row_data[headers[cell_index]] = rowspan_cells[cell_index][0]
                rowspan_cells[cell_index] = (
                    rowspan_cells[cell_index][0],
                    rowspan_cells[cell_index][1] - 1,
                )
                cell_index += 1  # Move to next cell header

            # If current cell has a rowspan
            rowspan_value = cell.get("rowspan")
            if rowspan_value:
                rowspan_length = int(rowspan_value) - 1  # Exclude the current row
                rowspan_cells[cell_index] = (cell, rowspan_length)

            # Store current row's cell element (not text content)
            row_data[headers[cell_index]] = cell
            cell_index += 1  # Move to the next header for each processed cell

        # Apply any remaining rowspan data
        while cell_index < len(headers):
            if cell_index in rowspan_cells and rowspan_cells[cell_index][1] > 0:
                row_data[headers[cell_index]] = rowspan_cells[cell_index][0]
                rowspan_cells[cell_index] = (
                    rowspan_cells[cell_index][0],
                    rowspan_cells[cell_index][1] - 1,
                )
            cell_index += 1

        yield row_data
