import re
from lxml import html
from zavod import Context
from typing import Dict
from urllib.parse import urljoin

from zavod.shed.zyte_api import fetch_html


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
    # crime = row.pop("Crime")
    # birth_place = row.pop("Birth Place")
    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "wanted")
    # entity.add("notes", crime, lang="spa")
    # entity.add("birthPlace", clean_birth_place(birth_place), lang="spa")
    # Emit the entities
    context.emit(entity, target=True)

    # Log warnings if there are unhandled fields remaining in the dict
    # context.audit_data(row)


# # Parse the table and yield rows as dictionaries.
# def parse_card(card: html.HtmlElement) -> Dict[str, str]:
#     name_element = card.xpath(
#         ".//p[strong[contains(text(), 'Nombre:')]]/strong/following-sibling::text()"
#     )
#     crime_element = card.xpath(
#         ".//p[strong[contains(text(), 'Delito:')]]/strong/following-sibling::text()"
#     )

#     name = name_element[0].strip() if name_element else None
#     # Join all lines of the crime_element, remove excessive whitespace characters
#     crime_text = (
#         " ".join(text.strip() for text in crime_element) if crime_element else None
#     )
#     birth_place = None
#     if crime_text:
#         match = re.search(r"Lugar de nacimiento\s*:\s*(.*)", crime_text)
#         if match:
#             birth_place = match.group(1).strip()
#             # Remove the birth place part from the crime text
#             crime_text = crime_text[: match.start()].strip()

#     return {"Name": name, "Crime": crime_text, "Birth Place": birth_place}


def parse_card(card: html.HtmlElement, context: Context) -> Dict[str, str]:
    # Extract link
    link_element = card.xpath(
        ".//a[contains(@href, 'colabora_masbucados_detalle.php')]/@href"
    )
    link = urljoin(context.data_url, link_element[0]) if link_element else None

    # Extract names
    first_name_element = card.xpath(
        ".//h5[@class='card-title text-center']/div[1]/text()"
    )
    last_name_element = card.xpath(
        ".//h5[@class='card-title text-center']/div[2]/text()"
    )
    first_name = first_name_element[0].strip() if first_name_element else None
    last_name = last_name_element[0].strip() if last_name_element else None
    full_name = f"{first_name} {last_name}" if first_name and last_name else None

    # Extract description
    description_element = card.xpath(".//p[@class='m-0  text-center']/text()")
    description = description_element[0].strip() if description_element else None

    return {
        "Link": link,
        "Name": full_name,
        "Description": description,
    }


def unblock_validator(doc: html.HtmlElement) -> bool:
    return len(doc.xpath(".//div[contains(@class, 'card-body')]")) > 0


def crawl(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=3)
    # Find all <div class="card-body">
    cards = doc.xpath(".//div[contains(@class, 'card-body')]")
    if not cards:
        context.log.warn("No cards found in the document.")
        return
    for card in cards:
        #  context.log.info(f"Processing card: {html.tostring(card).decode('utf-8')}")
        row = parse_card(card, context)
        if row["Name"] and row["Description"]:  # Ensure both fields are present
            crawl_item(context, row)
        else:
            context.log.warn(f"Skipping a card with missing fields: {row}")
