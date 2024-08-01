from lxml import html
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, List, Any


def crawl_item(context: Context, row: Dict[str, Any]):
    # Create the entity based on the schema
    internal_id = row.pop("id")
    name = row.pop("entities")
    if name in 
    date_str = row.pop("date")
    merchandise = row.get("merchandise")
    status = row.get("status")
    status_notes = row.get("status_notes")
    listing_date = h.parse_date((date_str), formats="%m/%d/%Y")

    entity = context.make("Company")
    entity.id = context.make_id(name, internal_id)
    entity.add("name", name)
    entity.add("idNumber", id)
    entity.add("notes", listing_date)
    entity.add("notes", merchandise)
    if status in ["Active", "Partially Active"]:
        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)
        sanction.add("listingDate", listing_date)
    entity.add("notes", status_notes)
    context.emit(entity, target=True)  # Emit the entity


def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    """Parse the HTML table and yield rows as dictionaries."""
    headers: List[str] = []

    # First, grab the headers from the second row (after skipping the title row)
    header_row = table.findall(".//tr")[1]  # Access the second row (index 1)

    # Extract headers from the header row
    for el in header_row.findall("./td"):
        headers.append(slugify(el.text_content().strip()))

    # Now proceed to parse the body rows
    for row in table.findall(".//tr")[
        2:
    ]:  # Start from the third row (index 2), after the headers
        cells = row.findall("./td")  # Get all cell elements in the current row

        if len(cells) == 0:
            continue  # Skip empty rows

        if len(headers) == len(cells):  # Ensure header and cell count match
            yield {
                "id": cells[0].text_content().strip(),
                "date": cells[1].text_content().strip(),
                "merchandise": cells[2].text_content().strip(),
                "entities": cells[3].text_content().strip(),
                "status": cells[4].text_content().strip(),
                "status_notes": cells[5].text_content().strip(),
            }


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    table = doc.xpath(
        "//table[contains(@class, 'usa-table')]"
    )  # Use XPath to find the desired table
    if isinstance(table, list) and table:
        for row in parse_table(table[0]):  # First table found
            crawl_item(context, row)
