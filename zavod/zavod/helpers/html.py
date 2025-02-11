from typing import Dict, Generator, Optional, cast
from normality import slugify, collapse_spaces
from lxml.html import HtmlElement


def parse_html_table(
    table: HtmlElement,
    header_tag: str = "th",
    skiprows: int = 0,
) -> Generator[Dict[Optional[str], HtmlElement], None, None]:
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
