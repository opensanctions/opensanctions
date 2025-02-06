from lxml import html
from normality import slugify
from typing import Dict
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    sourceUrl = row.pop("link")
    description = row.pop("description")
    entity = context.make("Person")
    entity.id = context.make_id(first_name, sourceUrl, description)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("sourceUrl", sourceUrl)
    entity.add("description", description, lang="spa")
    entity.add("topics", "wanted")

    doc = context.fetch_html(sourceUrl)  # Fetch the page
    details_section = doc.find('.//dl[@class="row"]')  # Find the <dl> section

    # Initialize facts to collect key-value pairs
    facts = {}
    if details_section is not None:
        for element in details_section.getchildren():
            # If it's a <dt>, set it as a key (e.g., 'Nombre y apellidos')
            if element.tag == "dt":
                facts_key = slugify(element.text, sep="_")
            # If it's a <dd>, set it as the value for the last key (e.g., 'Marek Dawid LEGIEC')
            if element.tag == "dd" and facts_key:
                facts[facts_key] = element.text.strip()
                facts_key = None  # Reset key after each value

        # Add additional fields from the facts dictionary to the entity
        for field, value in facts.items():
            if field == "informacion":
                entity.add("notes", value, lang="spa")

    context.emit(entity)
    context.audit_data(row)


def parse_link_element(
    link_element: html.HtmlElement, context: Context
) -> Dict[str, str]:
    # Extract link directly
    link = urljoin(context.data_url, link_element.attrib["href"])

    # Extract names from the card element inside the container
    card = link_element.xpath(".//div[contains(@class, 'card-body')]")[0]
    name_elements = card.xpath(".//h5[@class='card-title text-center']/text()")
    cleaned_names = [name.strip() for name in name_elements]
    first_name, last_name = cleaned_names

    # Adjusted XPath to ignore multiple spaces in the class attribute
    description_element = card.xpath(".//p[contains(@class, 'text-center')]/text()")
    description = description_element[0].strip()

    return {
        "link": link,
        "first_name": first_name,
        "last_name": last_name,
        "description": description,
    }


def crawl(context: Context):
    link_xpath = ".//a[starts-with(@href, 'colabora_masbucados_detalle')]"
    doc = fetch_html(context, context.data_url, link_xpath, cache_days=1)
    # Find all <a> elements with the specified pattern
    link_elements = doc.xpath(link_xpath)
    if not link_elements:
        context.log.warn("No link elements found in the document.")
        return
    for link_element in link_elements:
        row = parse_link_element(link_element, context)
        if row["first_name"] and row["description"]:  # Ensure both fields are present
            crawl_item(context, row)
        else:
            context.log.warn(f"Skipping a container with missing fields: {row}")
