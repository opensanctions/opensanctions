from typing import Dict, Generator
from normality import slugify, collapse_spaces
from zavod.util import ElementOrTree


def parse_table(table: ElementOrTree) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(el.text_content()) for el in row.findall("./th")]
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}
