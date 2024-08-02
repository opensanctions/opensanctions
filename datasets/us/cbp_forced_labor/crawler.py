from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, Any
from lxml.html import HtmlElement


def crawl_item(context: Context, row: Dict[str, Any]):
    try:
        # Ensure all necessary keys are in the row
        # Map the '#' header to 'id'
        if "id" in row:
            row["id"] = row.pop("id")
        elif None in row:
            row["id"] = row.pop(None)

        if "status-notes" in row:
            row["status_notes_text"] = row.pop("status-notes")

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
        # status_notes_link = row.pop("status_notes_link")
        # print(f"Processing entity: {name}, ID: {internal_id}")

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
                # entity.add("notes", status_notes_link)
                # print(f"Emitting entity: {entity}")
                context.emit(entity, target=True)  # Emit the entity\

    except Exception as e:
        print(f"Error processing row {row}: {e}")
        context.log.error(f"Error processing row {row}: {e}")


def parse_table(
    table: HtmlElement, main_header: str
) -> Generator[Dict[str, str], None, None]:
    headers = []
    header_found = False

    for row in table.findall(".//tr"):
        # Check if this row is the main header row
        main_header_cell = row.find(".//th[@colspan='6'][@scope='col']")
        if main_header_cell is not None:
            continue  # Skip the main header row as it's already provided

        # Otherwise, check if it's the detailed header row
        if not header_found:
            for el in row.findall(".//td/p/strong"):
                header_text = el.text_content().strip()
                # Handle special case for 'id'
                if header_text == "#":
                    header_text = "id"
                headers.append(slugify(header_text))
            header_found = True
            # print(f"Headers: {headers}")
            continue

        # Proceed to parse data rows
        cells = row.findall(".//td")
        if len(cells) == 0:
            continue  # Skip empty rows
        if len(headers) != len(cells):  # Ensure headers and cells match
            print(
                f"Header-cell mismatch: headers {headers}, cells {[c.text_content().strip() for c in cells]}"
            )
            continue  # Skip rows where headers and cells do not match

        row_data = {
            header: cell.text_content().strip() for header, cell in zip(headers, cells)
        }
        row_data["main_header"] = main_header  # Add main_header to each row
        # print(f"Parsed row: {row_data}")
        yield row_data


def crawl(context: Context):
    print("Fetching and parsing the HTML document...")
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    # print("Iterating over tables and rows...")

    for accordion in doc.xpath("//div[contains(@class, 'usa-section-accordion')]"):
        heading_el = accordion.find(".//h2")
        heading_text = heading_el.text_content()
        table = accordion.find(".//table")
        # print(heading_text, table)
        for item in parse_table(table, main_header=heading_text):
            # print(f"Processing row: {item}")
            crawl_item(context, item)
    # print("Finished processing all tables.")
