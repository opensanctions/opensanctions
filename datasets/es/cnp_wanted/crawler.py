from lxml import html
from zavod import Context, helpers as h
from typing import Dict


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("Name")
    crime = row.pop("Crime")
    entity = context.make("Person")
    entity.id = context.make_id(name, crime)
    entity.add("name", name)
    entity.add("topics", "wanted")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", crime)
    sanction.add("program", "Most Wanted")

    # Emit the entities
    context.emit(entity, target=True)
    context.emit(sanction)

    # Log warnings if there are unhandled fields remaining in the dict
    context.audit_data(row)


# Parse the table and yield rows as dictionaries.
def parse_card(card: html.HtmlElement) -> Dict[str, str]:
    name_element = card.xpath(".//p[1]/text()")
    crime_element = card.xpath(".//p[2]/text()")

    name = name_element[0].replace("Nombre: ", "").strip() if name_element else None
    crime = crime_element[0].replace("Delito: ", "").strip() if crime_element else None

    return {"Name": name, "Crime": crime}


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)

    # Find all <div class="card-body">
    cards = doc.xpath(".//div[contains(@class, 'card-body')]")

    if not cards:
        context.log.warn("No cards found in the document.")
        return

    for card in cards:
        context.log.info(
            f"Processing card: {html.tostring(card, pretty_print=True).decode('utf-8')}"
        )
        row = parse_card(card)
        crawl_item(context, row)


#     for card in cards:
#         row = parse_card(card)
#         if row["Name"] and row["Crime"]:  # Ensure both fields are present
#             crawl_item(context, row)
#         else:
#             context.log.warn(f"Skipping a card with missing fields: {row}")
