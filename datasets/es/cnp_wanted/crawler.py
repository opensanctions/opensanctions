from lxml import html
from zavod import Context
from typing import Dict
from urllib.parse import urljoin

from zavod.shed.zyte_api import fetch_html


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("Name")
    sourceUrl = row.pop("Link")
    description = row.pop("Description")
    entity = context.make("Person")
    entity.id = context.make_id(name, sourceUrl, description)
    entity.add("name", name)
    entity.add("sourceUrl", sourceUrl)
    entity.add("description", description, lang="spa")
    entity.add("topics", "wanted")
    context.emit(entity, target=True)

    # Log warnings if there are unhandled fields remaining in the dict
    context.audit_data(row)


def parse_card(card: html.HtmlElement, context: Context) -> Dict[str, str]:
    # Extract link
    # link_element = card.xpath(
    #     ".//a[contains(@href, 'colabora_masbucados_detalle')]/@href"
    # )
    link_element = card.xpath(".//a[starts-with(@href, 'colabora_masbucados_detalle')]")
    link = urljoin(context.data_url, link_element[0]) if link_element else None

    # Extract names
    first_name_element = card.xpath(
        ".//h5[@class='card-title text-center']/div[1]/text()"
    )
    last_name_element = card.xpath(
        ".//h5[@class='card-title text-center']/div[2]/text()"
    )
    first_name = first_name_element[0].strip()
    last_name = last_name_element[0].strip()
    full_name = f"{first_name} {last_name}"

    # Extract description
    description_element = card.xpath(".//p[@class='m-0  text-center']/text()")
    description = description_element[0].strip()

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
