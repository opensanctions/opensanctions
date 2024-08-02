from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, Any
from lxml.html import HtmlElement
import re

pattern = r"\bFishing\s+Vessels\b"


def crawl_item(context: Context, row: Dict[str, Any]):
    try:
        # Ensure all necessary keys are in the row
        # Map the special case headers to expected names
        if "#" in row:
            row["id"] = row.pop("#")
        # Extract required keys and handle them
        country = row.pop("main_header")
        internal_id = row.pop("id")
        name = row.pop("entities")
        name_result = context.lookup("name", name)
        listing_date = h.parse_date(row.pop("date"), formats=["%m/%d/%Y"])
        merchandise = row.pop("merchandise")
        status = row.pop("status")
        status_notes = row.pop("status-notes")
        status_notes_link = row.pop("status_notes_link", None)

        # print(f"Processing entity: {name}, ID: {internal_id}")

        if re.search(pattern, country, re.IGNORECASE):
            entity = context.make("Vessel")
            if name_result is not None and name_result.entities:
                for match_entity in name_result.entities:
                    if match_entity.get("name"):
                        entity.id = context.make_id(
                            match_entity.get("name"), internal_id
                        )
                        entity.add("name", match_entity.get("name"))
                        if status in ["Active", "Partially Active"]:
                            entity.add("topics", "sanction")
                            sanction = h.make_sanction(context, entity)
                            sanction.add("listingDate", listing_date)
                            entity.add("notes", status_notes)
        else:
            entity = context.make("LegalEntity")
            if name_result is not None and name_result.entities:
                for match_entity in name_result.entities:
                    if match_entity.get(
                        "name"
                    ):  # create multiple entries for each entity
                        entity.id = context.make_id(
                            match_entity.get("name"), internal_id
                        )
                        entity.add("name", match_entity.get("name"))

                        if "alias" in match_entity:
                            entity.add("alias", match_entity.get("alias"))
                    entity.add("idNumber", internal_id)
                    entity.add("sector", merchandise)
                    if country:
                        entity.add("country", country)
                    if status in ["Active", "Partially Active"]:
                        entity.add("topics", "sanction")
                        sanction = h.make_sanction(context, entity)
                        sanction.add("listingDate", listing_date)
                        entity.add("notes", status_notes)
                    if status_notes_link:
                        entity.add("notes", status_notes_link)

        context.emit(entity, target=True)
    except Exception as e:
        print(f"Error processing row {row}: {e}")


def parse_table(
    table: HtmlElement, main_header: str
) -> Generator[Dict[str, Any], None, None]:
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

        # Look for link near status-notes
        for cell in cells:
            link = cell.find(".//a")
            if link is not None:
                link_url = link.get("href")
                if (
                    "status-notes" in row_data
                    and cell.text_content().strip() == row_data["status-notes"]
                ):
                    row_data["status_notes_link"] = link_url
        yield row_data


def crawl(context: Context):
    print("Fetching and parsing the HTML document...")
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for accordion in doc.xpath("//div[contains(@class, 'usa-section-accordion')]"):
        heading_el = accordion.find(".//h2")
        heading_text = heading_el.text_content()
        table = accordion.find(".//table")

        for item in parse_table(table, main_header=heading_text):
            crawl_item(context, item)
