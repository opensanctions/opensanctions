from typing import Dict, Generator, cast
from normality import slugify, collapse_spaces
from lxml.html import HtmlElement

from zavod.util import ElementOrTree


def parse_table(table: HtmlElement) -> Generator[Dict[str, str], None, None]:
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

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}
