from typing import Dict, Generator, Optional, Set, cast
from normality import slugify, squash_spaces
from lxml.html import HtmlElement
from zavod.logs import get_logger


log = get_logger(__name__)


def parse_html_table(
    table: HtmlElement,
    header_tag: str = "th",
    skiprows: int = 0,
    ignore_colspan: Optional[Set[str]] = None,
) -> Generator[Dict[str, HtmlElement], None, None]:
    """
    Parse an HTML table into a generator yielding a dict for each row.

    Args:
        table
        header_tag: Default th, allows treating td as header
        skiprows: Number of rows to skip before expecting the header row.
        ignore_colspan: colspans to ignore, e.g. when a full span means a subheading

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
                header_text = slugify(eltree.text_content(), sep="_")
                assert header_text is not None, "No table header text"
                headers.append(header_text)
            continue

        cells = row.findall("./td")
        if len(headers) != len(cells):
            str_cells = [c.text_content() for c in cells]
            colspans = set([c.get("colspan") for c in cells])
            if ignore_colspan and colspans == set(ignore_colspan):
                log.info(f"Ignoring row {rownum} with colspan: {str_cells}")
                continue
            else:
                msg = f"Expected {len(headers)} cells, found {len(cells)} on row {rownum} {str_cells}"
                assert len(headers) == len(cells), msg

        yield {hdr: c for hdr, c in zip(headers, cells)}


def cells_to_str(row: Dict[str, HtmlElement]) -> Dict[str, str | None]:
    """
    Return the string value of each HtmlElement value in the passed dictionary

    Useful when all you need is the string value of each cell in a table row.
    """
    return {
        # Empty cells are None, not the empty string
        k: squash_spaces(v.text_content()) or None
        for k, v in row.items()
    }


def links_to_dict(el: HtmlElement) -> Dict[str | None, str | None]:
    """
    Return a dictionary of the text content and href of each anchor element in the
    passed HtmlElement

    Useful for when the link labels are consistent and can be used as keys
    """
    return {
        slugify(a.text_content(), sep="_"): a.get("href") for a in el.findall(".//a")
    }
