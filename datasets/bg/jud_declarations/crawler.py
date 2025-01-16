from typing import Dict, Generator, List, Optional
from lxml.html import HtmlElement


from zavod import Context, helpers as h


def parse_html_table(
    table: HtmlElement,
    skiprows: int = 0,
    headers: Optional[List[str]] = None,
) -> Generator[Dict[str, HtmlElement], None, None]:
    for rownum, row in enumerate(table.findall(".//tr")):
        if rownum < skiprows:
            continue
        cells = row.findall("./td")
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    alphabet_links = doc.xpath(".//div[@itemprop='articleBody']/p//a[@href]")
    for a in alphabet_links[:1]:
        doc.make_links_absolute(context.data_url)
        link = a.get("href")
        print(link)
        doc = context.fetch_html(link, cache_days=1)
        table = doc.xpath(".//table")
        assert len(table) == 1
        table = table[0]
        for row in parse_html_table(table, headers=["name", "incoming_number"]):
            row = h.cells_to_str(row)
            name = row.pop("name")
            incoming_number = row.pop("incoming_number")

            person = context.make("Person")
            person.id = context.make_id(name, incoming_number)
            person.add("name", name, lang="bul")

            context.emit(person)
