from typing import Dict, Generator, cast
from normality import slugify, collapse_spaces
from lxml.html import HtmlElement


def parse_html_table(
    table: HtmlElement,
    header_tag: str = "th",
    skiprows: int = 0,
) -> Generator[Dict[str, HtmlElement], None, None]:
    """
    Parse an HTML table into a generator yielding a dict for each row.

    Returns:
        Generator of dict per row, where the keys are the _-slugified table headings
            and the values are the HtmlElement of the cell.

    See also:
      - `zavod.helpers.cells_to_str`
      - `zavod.helpers.links_to_dict`
    """
    headers = None
    for rownum, row in enumerate(table.findall(".//tr")):
        if rownum < skiprows:
            continue

        if headers is None:
            headers = []
            for el in row.findall(f"./{header_tag}"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content(), sep="_"))
            continue

        cells = row.findall("./td")
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def parse_html_table_links(
    table: HtmlElement,
    header_tag: str = "th",
    skiprows: int = 0,
    base_url: str = "",
    link_columns: list[str] = [],
) -> Generator[Dict[str, str | Dict[str, str]], None, None]:
    """
    Parse an HTML table into a generator yielding a dict for each row.

    Args:
        table: The HTML table element to parse.
        header_tag: The tag used for table headers (default: "th").
        skiprows: The number of initial rows to skip (default: 0).
        base_url: The base URL to resolve relative links (default: "").
        link_columns: List of column names where both text and links should be extracted (default: None).

    Returns:
        Generator of dict per row, where:
            - For columns in `link_columns`, values are dictionaries containing 'text' and 'link' keys.
            - For other columns, values are plain text.
    """
    headers = None

    for rownum, row in enumerate(table.findall(".//tr")):
        if rownum < skiprows:
            continue

        if headers is None:
            headers = []
            for el in row.findall(f"./{header_tag}"):
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content(), sep="_"))
            continue

        cells = row.findall("./td")
        assert len(headers) == len(cells), (headers, cells)

        row_data = {}
        for hdr, cell in zip(headers, cells):
            # Extract text content
            cell_text = cell.text_content().strip()

            # Extract link if the column is specified in link_columns
            if hdr in link_columns:
                link_element = cell.find(".//a[@href]")
                cell_link = None
                if link_element is not None:
                    link_element.make_links_absolute(base_url)
                    cell_link = link_element.get("href")

                # Store both text and link in a dictionary
                row_data[hdr] = {
                    "text": cell_text,
                    "link": cell_link,
                }
            else:
                # For columns without links, store only the text
                row_data[hdr] = cell_text

        yield row_data


def cells_to_str(row: Dict[str, HtmlElement]) -> Dict[str, str | None]:
    """
    Return the string value of each HtmlElement value in the passed dictionary

    Useful when all you need is the string value of each cell in a table row.
    """
    return {k: collapse_spaces(v.text_content()) for k, v in row.items()}


def links_to_dict(el: HtmlElement) -> Dict[str | None, str | None]:
    """
    Return a dictionary of the text content and href of each anchor element in the
    passed HtmlElement

    Useful for when the link labels are consistent and can be used as keys
    """
    return {
        slugify(a.text_content(), sep="_"): a.get("href") for a in el.findall(".//a")
    }
