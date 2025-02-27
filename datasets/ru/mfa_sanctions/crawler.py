import re
from typing import Dict, Generator, List, Optional
from lxml.html import HtmlElement

from zavod import Context, helpers as h

# US citizens under personal sanctions, including a ban on entry into the Russian Federation
PROGRAM = "Граждане США, находящиеся под персональными санкциями, включая запрет на въезд в Российскую Федерацию"


def parse_html_table(
    table: HtmlElement,
    skiprows: int = 0,
    headers: Optional[List[str]] = None,
) -> Generator[Dict[str, HtmlElement], None, None]:
    for rownum, row in enumerate(table.findall(".//tr")):
        if rownum < skiprows:
            continue

        cells = row.findall("./td")
        # Skip alphabetical headers
        if any(cell.get("colspan") == "4" for cell in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    table = doc.xpath(".//table")
    assert len(table) == 1
    table = table[0]

    for row in parse_html_table(table, headers=["index", "name", "-", "position"]):
        row = h.cells_to_str(row)
        name_raw = row.pop("name").rstrip("–").rstrip(",").strip()
        assert re.match(r"^[\S\s]+\s\(.+\)$", name_raw), name_raw
        position = row.pop("position")

        person = context.make("Person")
        person.id = context.make_id(name_raw, position)
        name, name_en = h.multi_split(name_raw, [" ("])
        person.add("name", name, lang="rus")
        person.add("name", name_en.rstrip(")"), lang="eng")
        person.add("position", position.split(", "), lang="rus")
        person.add("citizenship", "us")
        person.add("topics", "sanction.counter")

        sanction = h.make_sanction(context, person, program_key="RU-MFA")
        sanction.add("program", PROGRAM, lang="rus")

        context.emit(person)
        context.emit(sanction)
