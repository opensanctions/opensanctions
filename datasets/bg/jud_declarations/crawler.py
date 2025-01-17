from typing import Dict, Generator, List, Optional
from lxml.html import HtmlElement

from zavod import Context, helpers as h
from zavod.logic.pep import categorise, OccupancyStatus


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
    # Bulgarian alphabet has 30 letters, but the name can start only with 29 of them
    # We want to cover the last 2 years at any time
    for a in alphabet_links[:58]:
        doc.make_links_absolute(context.data_url)
        link = a.get("href")
        doc = context.fetch_html(link, cache_days=1)
        table = doc.xpath(".//table")
        if len(table) == 0:
            context.log.warning("No tables found")
            continue
        assert len(table) == 1
        table = table[0]
        for row in parse_html_table(table, headers=["name", "doc_id_date"]):
            doc.make_links_absolute(context.data_url)
            str_row = h.cells_to_str(row)
            name = str_row.pop("name")
            doc_id_date = str_row.pop("doc_id_date")
            # Skip the header row
            if name == "Име" and doc_id_date == "Входящ номер":
                continue
            if len(doc_id_date.split("/")) == 2:
                _, date = doc_id_date.split("/")
            else:
                date = context.lookup_value("doc_id_date", doc_id_date)
                if date is None:
                    context.log.warning(f"Invalid doc_id_date: {doc_id_date}")
                continue
            # Link is in the same cell as the name
            name_link_elem = HtmlElement(row["name"]).find(".//a")
            declaration_url = name_link_elem.get("href")

            person = context.make("Person")
            # We want the same person for 2 different years to have the same ID
            person.id = context.make_id(name)
            person.add("name", name, lang="bul")
            person.add("topics", "role.pep")
            person.add("sourceUrl", declaration_url)

            position = h.make_position(
                context,
                name="Judiciary Official",
                lang="bul",
                country="BG",
            )

            categorisation = categorise(context, position, is_pep=True)
            if not categorisation.is_pep:
                return

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                categorisation=categorisation,
                status=OccupancyStatus.UNKNOWN,
            )
            # Switch to the declarationDate once it's introduced
            h.apply_date(occupancy, "date", date)

            if occupancy is not None:
                context.emit(position)
                context.emit(occupancy)
                context.emit(person)
