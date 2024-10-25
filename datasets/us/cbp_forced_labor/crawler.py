from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, Any
from lxml.html import HtmlElement
import re

REGEX_VESSEL = re.compile(r"\bFishing\s+Vessels\b")


def crawl_item(context: Context, row: Dict[str, Any]):
    # Map the special case headers to expected names
    if "#" in row:
        row["id"] = row.pop("#")
    # Extract required keys and handle them
    country = row.pop("main_header")
    if REGEX_VESSEL.search(country, re.IGNORECASE):
        crawl_vessel(context, row)
    else:
        crawl_company(context, row, country)


def crawl_vessel(context: Context, row: Dict[str, Any]):
    # if REGEX_VESSEL.search(country, re.IGNORECASE):
    if "#" in row:
        row["id"] = row.pop("#")
    internal_id = row.pop("id")
    name = row.pop("entities")
    name_result = context.lookup("name", name)
    listing_date = row.pop("date")
    status = row.pop("status")
    status_notes = row.pop("status-notes")
    status_notes_link = row.pop("status_notes_link", None)
    name_result = context.lookup("name", name)
    if name_result is None:
        context.log.warning("No name found for company", name_result=name)
        return
    for match_entity in name_result.entities:
        if not match_entity.get("name"):
            context.log.warning("No name found for vessel", entity=match_entity)
            continue
        entity = context.make("Vessel")
        entity.id = context.make_id(match_entity.get("name"), internal_id)
        for prop, value in match_entity.items():
            entity.add(prop, value)
        entity.add("notes", status_notes)
        entity.add("notes", status_notes_link)
        if status in ["Active", "Partially Active"]:
            is_active = True
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            h.apply_date(sanction, "listingDate", listing_date)
            context.emit(sanction)
        else:
            is_active = False
        context.emit(entity, target=is_active)


def crawl_company(context: Context, row: Dict[str, Any], country: str):
    internal_id = row.pop("id")
    name = row.pop("entities")
    name_result = context.lookup("name", name)
    listing_date = row.pop("date")
    merchandise = row.pop("merchandise")
    status = row.pop("status")
    status_notes = row.pop("status-notes")
    status_notes_link = row.pop("status_notes_link", None)
    name_result = context.lookup("name", name)
    if name_result is None:
        context.log.warning("No name found for company", name_result=name)
        return
    for match_entity in name_result.entities:
        if not match_entity.get("name"):
            context.log.warning("No name found for a company", entity=match_entity)
            continue
        entity = context.make("LegalEntity")  # create multiple entries for each entity
        entity.id = context.make_id(match_entity.get("name"), internal_id)
        for prop, value in match_entity.items():
            entity.add(prop, value)
        entity.add("notes", status_notes)
        entity.add("notes", status_notes_link)
        entity.add("idNumber", internal_id)
        entity.add("sector", merchandise)
        if country:
            entity.add("country", country)
        if status in ["Active", "Partially Active"]:
            is_active = True
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            h.apply_date(sanction, "listingDate", listing_date)
            context.emit(sanction)
        else:
            is_active = False
        context.emit(entity, target=is_active)


def parse_table(
    context: Context, table: HtmlElement, main_header: str
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
            context.log.warning("No cells found in the row.", cells=cells)
        if len(headers) != len(cells):
            context.log.warning(
                "Header-cell mismatch.",
                headers=headers,
                cells=[c.text_content().strip() for c in cells],
            )

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
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for accordion in doc.xpath("//div[contains(@class, 'usa-section-accordion')]"):
        heading_el = accordion.find(".//h2")
        heading_text = heading_el.text_content()
        table = accordion.find(".//table")

        for item in parse_table(context, table, main_header=heading_text):
            crawl_item(context, item)
