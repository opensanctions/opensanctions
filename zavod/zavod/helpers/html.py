from typing import Dict, Generator, cast
from normality import slugify, collapse_spaces
from lxml.html import HtmlElement


def parse_table(table: HtmlElement, header_tag: str = "th") -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall(f"./{header_tag}"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content(), sep="_"))
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}
