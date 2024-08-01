from lxml import html
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, List, Any


def crawl_item(context: Context, row: Dict[str, Any]):
    try:
        # Create the entity based on the schema
        country = row.pop("main_header")
        internal_id = row.pop("id")
        name = row.pop("entities")
        name_result = context.lookup("name", name)
        listing_date = h.parse_date(
            row.pop("date"), formats=["%m/%d/%Y"]
        )  # "%d/%m/%Y"])
        merchandise = row.pop("merchandise")
        status = row.pop("status")
        status_notes = row.pop("status_notes_text")
        status_notes_link = row.pop("status_notes_link")
        print(f"Processing entity: {name}, ID: {internal_id}")

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
                print(f"Emitting entity: {entity}")
                context.emit(entity, target=True)  # Emit the entity\

    except Exception as e:
        print(f"Error processing row {row}: {e}")
        context.log.error(f"Error processing row {row}: {e}")


def parse_table(doc: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    """Parse multiple HTML tables in the document and yield rows as dictionaries."""
    table_ids = [
        "accordion-33324",
        "accordion-33328",
        "accordion-33330",
        "accordion-33332",
        "accordion-33334",
        "accordion-33336",
        "accordion-33338",
        "accordion-33340",
        "accordion-33342",
    ]

    for table_id in table_ids:
        # Fetch each table using its specific ID
        table = doc.xpath(
            f"//*[@id='{table_id}']//table[contains(@class, 'usa-table')]"
        )
        if not table:
            print(f"No table found for ID: {table_id}")
            continue  # Skip if no table is found

        table = table[0]  # Extract the first occurrence for this ID
        print(f"Processing table with ID: {table_id}")
        headers: List[str] = []

        # Capture the main header to identify the country or section
        main_header_row = table.xpath(".//thead/tr/th")
        main_header = (
            main_header_row[0].text_content().strip() if main_header_row else ""
        )
        print(f"Main header found: {main_header}")

        # Then, grab the headers from the second row (after the main header)
        header_row = table.xpath(".//tr[2]")[0]  # Access the second row (index 1)

        # Extract headers from the header row
        for index, el in enumerate(header_row.findall("./td")):
            if index == 0:
                headers.append("#")
            else:
                if el is not None:
                    header_text = el.text_content().strip()
                    if header_text:
                        headers.append(slugify(header_text))
        print(f"Headers identified: {headers}")
        # Now proceed to parse the body rows
        for row in table.xpath(".//tr[position() > 2]"):  # Start from the third row
            cells = row.findall("./td")
            if len(cells) == 0:
                continue  # Skip empty rows
            if len(headers) == len(cells):  # Ensure header and cell count match
                id_cell = cells[0].find(".//p")
                id_text = id_cell.text_content().strip() if id_cell is not None else ""
                status_notes_cell = cells[5]
                status_notes_text = status_notes_cell.text_content().strip()
                anchor = status_notes_cell.find(".//a")
                status_notes_link = anchor.get("href") if anchor is not None else ""
                entity_data = {
                    "main_header": main_header,
                    "id": id_text,
                    "date": cells[1].text_content().strip(),
                    "merchandise": cells[2].text_content().strip(),
                    "entities": cells[3].text_content().strip(),
                    "status": cells[4].text_content().strip(),
                    "status_notes_text": status_notes_text,
                    "status_notes_link": status_notes_link,
                }
                print(f"Yielding entity data: {entity_data}")
                yield entity_data


def crawl(context: Context):
    print("Fetching and parsing the HTML document...")
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    print("Iterating over tables and rows...")
    # Iterate over all the rows yielded by parse_table
    for item in parse_table(doc):
        print(f"Processing row: {item}")
        crawl_item(context, item)
    print("Finished processing all tables.")
