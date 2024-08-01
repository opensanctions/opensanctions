from lxml import html
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, List, Any


def crawl_item(context: Context, row: Dict[str, Any]):
    # Create the entity based on the schema
    country = row.pop("main_header")
    internal_id = row.pop("id")
    name = row.pop("entities")
    name_result = context.lookup("name", name)
    listing_date = h.parse_date(row.pop("date"), formats=["%m/%d/%Y"])  # "%d/%m/%Y"])
    merchandise = row.pop("merchandise")
    status = row.pop("status")
    status_notes = row.pop("status_notes_text")
    status_notes_link = row.pop("status_notes_link")

    if name_result:
        for match_entity in name_result.entities:
            entity = context.make("LegalEntity")
            entity.id = context.make_id(match_entity.get("name"), internal_id)
            entity.add("name", match_entity.get("name"))
            # Safely add alias information if available
            assert "name" in match_entity, match_entity
            entity.add("alias", match_entity.get("alias"))
            entity.add("idNumber", internal_id)
            entity.add("sector", merchandise)
            entity.add("country", country)

            if status in ["Active", "Partially Active"]:
                entity.add("topics", "sanction")
                sanction = h.make_sanction(context, entity)
                sanction.add("listingDate", listing_date)
            entity.add("notes", status_notes)
            entity.add("notes", status_notes_link)
            context.emit(entity, target=True)  # Emit the entity


def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    """Parse the HTML table and yield rows as dictionaries."""
    headers: List[str] = []

    # Grab the main header from the first row
    main_header_row = table.find(".//tr")  # The first row (index 0)
    main_header = main_header_row.find(".//th")

    # Then, grab the headers from the second row (after skipping the title row)
    header_row = table.findall(".//tr")[1]  # Access the second row (index 1)

    # Extract headers from the header row
    for index, el in enumerate(header_row.findall("./td")):
        # Retain the original header for the first column (the ID)
        if index == 0:
            headers.append("#")  # Keep the original header as is for ID
        else:
            if el is not None:
                header_text = el.text_content().strip()
                if header_text:
                    headers.append(slugify(header_text))  # Slugify other headers

    # Now proceed to parse the body rows
    for row in table.findall(".//tr")[2:]:  # Start from the third row (index 2)
        cells = row.findall("./td")  # Get all cell elements in the current row

        if len(cells) == 0:
            continue  # Skip empty rows

        if len(headers) == len(cells):  # Ensure header and cell count match
            status_notes_cell = cells[5]
            status_notes_text = status_notes_cell.text_content().strip()
            anchor = status_notes_cell.find(".//a")
            status_notes_link = anchor.get("href") if anchor is not None else ""

            yield {
                "main_header": main_header.text_content().strip(),
                "id": cells[0].find(".//p").text_content().strip(),
                "date": cells[1].text_content().strip(),
                "merchandise": cells[2].text_content().strip(),
                "entities": cells[3].text_content().strip(),
                "status": cells[4].text_content().strip(),
                "status_notes_text": status_notes_text,
                "status_notes_link": status_notes_link,
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
