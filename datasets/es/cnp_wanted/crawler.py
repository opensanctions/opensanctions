import re
from lxml import html
from zavod import Context
from typing import Dict


def clean_birth_place(birth_place: str) -> str:
    birth_place = re.sub(r"\s*-\s*", " -", birth_place)  # Standardize hyphen format
    # Remove trailing punctuation and standardize format
    birth_place = re.sub(r"[\s-]*[\.\-]+$", "", birth_place).strip()
    # Extract components using regex
    match = re.match(r"^(.*? \(.+?\))\s*-?\s*(.+?)$", birth_place)
    if match:
        city_area, country = match.groups()
        return f"{city_area.strip()}, {country.strip()}"
    else:
        # If regex does not match, return as-is
        return birth_place


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("Name")
    crime = row.pop("Crime")
    birth_place = row.pop("Birth Place")
    entity = context.make("Person")
    entity.id = context.make_id(name, birth_place)
    entity.add("name", name)
    entity.add("topics", "wanted")
    entity.add("notes", crime, lang="spa")
    entity.add("birthPlace", clean_birth_place(birth_place), lang="spa")
    # Emit the entities
    context.emit(entity, target=True)

    # Log warnings if there are unhandled fields remaining in the dict
    context.audit_data(row)


# Parse the table and yield rows as dictionaries.
def parse_card(card: html.HtmlElement) -> Dict[str, str]:
    name_element = card.xpath(
        ".//p[strong[contains(text(), 'Nombre:')]]/strong/following-sibling::text()"
    )
    crime_element = card.xpath(
        ".//p[strong[contains(text(), 'Delito:')]]/strong/following-sibling::text()"
    )

    name = name_element[0].strip() if name_element else None
    # Join all lines of the crime_element, remove excessive whitespace characters
    crime_text = (
        " ".join(text.strip() for text in crime_element) if crime_element else None
    )
    birth_place = None
    if crime_text:
        match = re.search(r"Lugar de nacimiento\s*:\s*(.*)", crime_text)
        if match:
            birth_place = match.group(1).strip()
            # Remove the birth place part from the crime text
            crime_text = crime_text[: match.start()].strip()

    return {"Name": name, "Crime": crime_text, "Birth Place": birth_place}


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    # Find all <div class="card-body">
    cards = doc.xpath(".//div[contains(@class, 'card-body')]")
    if not cards:
        context.log.warn("No cards found in the document.")
        return
    for card in cards:
        #  context.log.info(f"Processing card: {html.tostring(card).decode('utf-8')}")
        row = parse_card(card)
        if row["Name"] and row["Crime"]:  # Ensure both fields are present
            crawl_item(context, row)
        else:
            context.log.warn(f"Skipping a card with missing fields: {row}")
