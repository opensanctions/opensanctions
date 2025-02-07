from typing import Dict, Generator, List, Optional, cast
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


def xpath(
    doc: HtmlElement,
    path: str,
    exactly: Optional[int] = None,
    at_least: Optional[int] = None,
) -> List[HtmlElement] | HtmlElement | str | None:
    """
    Return the list of result of an XPath query on the passed HtmlElement,
    or the first result if exactly=1.

    Asserts that the number of results is as expected.
    """
    if exactly is None and at_least is None:
        raise ValueError("Must specify 'exactly' or 'at_least'")

    results = doc.xpath(path)

    if exactly is not None:
        assert len(results) == exactly, (path, len(results))
    if at_least is not None:
        assert len(results) >= at_least, (path, len(results))

    if exactly == 1:
        return results[0]
    return results
