from lxml import html
import requests
from urllib.parse import urljoin
from zavod import Context, helpers as h
from slugify import slugify
from typing import Dict, Generator, List


# Convert various date formats to 'YYYY-MM-DD'.
def convert_date(date_str: str) -> List[str]:
    formats = [
        "%m/%d/%Y",  # 'MM/DD/YYYY' format
        "%B %d, %Y",  # 'Month DD, YYYY' format
        "%d-%b-%y",  # 'DD-MMM-YY' format
    ]
    return h.parse_date(date_str, formats, default=None)


# Process each row and emit an entity and sanction record.
def crawl_item(context: Context, row: Dict[str, str]):
    entity = context.make("Company")
    name = row.pop("company")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("country", "us")
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.get("comments", "").replace("<br>", "\n"))
    sanction.add("endDate", row.get("grounds", "").replace("<br>", "\n"))
    sanction.add("country", "us")
    sanction.add("provisions", row.get("typeLabel", ""))
    sanction.add("startDate", convert_date(row.get("from", "")))
    sanction.add("endDate", convert_date(row.get("to", "")))

    context.emit(entity, target=True)
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [
                slugify(el.text_content(), separator="_")
                if el.text_content().strip()
                else "company"
                for el in row.findall("./th")
            ]
            continue
        cells = [el.text_content().strip() for el in row.findall("./td")]
        if len(cells) != len(headers):
            continue
        yield {header: cell for header, cell in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context):
    context.log.info("Fetching data from FinCen Special Measures List")
    response = requests.get(
        "https://www.fincen.gov/resources/statutes-and-regulations/311-and-9714-special-measures"
    )
    doc = html.fromstring(response.content)

    table = doc.get_element_by_id("special-measures-table")
    if table is not None:
        for row in parse_table(table):
            crawl_item(context, row)
    else:
        context.log.error("Table with id 'special-measures-table' not found.")
