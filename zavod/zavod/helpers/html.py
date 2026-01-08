from typing import Dict, Generator, List, Optional, Set, cast

from lxml.html import HtmlElement
from normality import slugify, squash_spaces
from rigour.text import text_hash

from zavod.logs import get_logger
from zavod.util import Element

log = get_logger(__name__)


def element_text(el: Element | None, squash: bool = True) -> str:
    """
    Return the text content of an HtmlElement, or an empty string if empty.

    Args:
        el: The HTML element to extract text from.
        squash: Whether to squash whitespace and newlines in the text content.

    Returns:
        The text content of the element, or an empty string if empty.
    """
    # Workaround because lxml-stubs doesn't yet support HtmlElement
    # https://github.com/lxml/lxml-stubs/pull/71
    if el is None:
        return ""
    try:
        text = str(cast(HtmlElement, el).text_content())
    except AttributeError:
        text = str(el.xpath("string()", smart_strings=False))

    if squash:
        text = squash_spaces(text)
    return text


def element_text_hash(el: Element) -> str:
    """
    Return a hash of the text content of an HtmlElement. Empty elements will return the sha1
    of no data (`da39a3ee5e6b4b0d3255bfef95601890afd80709`).

    Args:
        el: The HTML element to extract text from.

    Returns:
        A hash of the text content of the element
    """
    text = element_text(el)
    return text_hash(text)


def parse_html_table(
    table: Element,
    header_tag: str = "th",
    skiprows: int = 0,
    ignore_colspan: Optional[Set[str]] = None,
    slugify_headers: bool = True,
    index_empty_headers: bool = False,
) -> Generator[Dict[str, Element], None, None]:
    """
    Parse an HTML table into a generator yielding a dict for each row.

    Args:
        table: The table HTML element to parse
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
            for colnum, el in enumerate(row.findall(f"./{header_tag}")):
                header_text: Optional[str] = element_text(el)
                if slugify_headers:
                    header_text = slugify(header_text, sep="_")
                if index_empty_headers and not header_text:
                    header_text = f"column_{colnum}"
                assert header_text is not None, "No table header text"
                headers.append(header_text)
            continue

        cells = row.findall("./td")
        if len(headers) != len(cells):
            str_cells = [element_text(c) for c in cells]
            colspans = set([c.get("colspan") for c in cells])
            if ignore_colspan and colspans == set(ignore_colspan):
                log.info(f"Ignoring row {rownum} with colspan: {str_cells}")
                continue
            else:
                msg = f"Expected {len(headers)} cells, found {len(cells)} on row {rownum} {str_cells}"
                assert len(headers) == len(cells), msg

        yield {hdr: c for hdr, c in zip(headers, cells)}


def cells_to_str(row: Dict[str, Element]) -> Dict[str, str | None]:
    """
    Return the string value of each HtmlElement value in the passed dictionary

    Useful when all you need is the string value of each cell in a table row.
    """
    return {
        # Empty cells are None, not the empty string
        k: element_text(v) or None
        for k, v in row.items()
    }


def links_to_dict(el: Element) -> Dict[str | None, str | None]:
    """
    Return a dictionary of the text content and href of each anchor element in the
    passed HtmlElement

    Useful for when the link labels are consistent and can be used as keys
    """
    return {
        slugify(element_text(a), sep="_"): a.get("href") for a in el.findall(".//a")
    }


def xpath_elements(
    el: Element, xpath: str, *, expect_exactly: Optional[int] = None
) -> List[Element]:
    """
    Return a list of HtmlElement objects that match the given XPath expression.
    """
    result = el.xpath(xpath)
    if not isinstance(result, list) or not all(isinstance(r, Element) for r in result):
        raise ValueError(
            f"Expected list[Element] as result of xpath, got {type(result)}"
        )
    if expect_exactly is not None and len(result) != expect_exactly:
        raise ValueError(
            f"Expected {expect_exactly} elements, got {len(result)} for xpath {xpath!r}"
        )
    return [cast(Element, r) for r in result]


def xpath_strings(
    el: Element, xpath: str, *, expect_exactly: Optional[int] = None
) -> List[str]:
    """
    Return a list of strings that match the given XPath expression.
    """
    result = el.xpath(xpath)
    if not isinstance(result, list) or not all(isinstance(r, str) for r in result):
        raise ValueError(f"Expected list[str] as result of xpath, got {type(result)}")
    if expect_exactly is not None and len(result) != expect_exactly:
        raise ValueError(
            f"Expected {expect_exactly} elements, got {len(result)} for xpath {xpath!r}"
        )
    return [cast(str, r) for r in result]


def xpath_string(el: Element, xpath: str) -> str:
    return xpath_strings(el, xpath, expect_exactly=1)[0]


def xpath_href(el: Element, xpath: str) -> str:
    """
    Return the href attribute of an element matched by the given XPath expression.

    Args:
        el: The HTML element to search within.
        xpath: The XPath expression to match the element.
    """

    elements = xpath_elements(el, xpath, expect_exactly=1)
    href = elements[0].get("href")
    if href is None:
        raise ValueError(f"Element matched by {xpath!r} has no href attribute")
    return href
