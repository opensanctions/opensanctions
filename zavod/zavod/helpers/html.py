from typing import Dict, Generator, cast
from normality import slugify, collapse_spaces
from lxml.html import HtmlElement


def parse_table(table: HtmlElement) -> Generator[Dict[str, HtmlElement], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content()))
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


def links_to_dict(el: HtmlElement) -> Dict[str, str]:
    """
    Return a dictionary of the text content and href of each anchor element in the
    passed HtmlElement

    Useful for when the link labels are consistent and can be used as keys
    """
    return {slugify(a.text_content()): a.get("href") for a in el.findall(".//a")}
