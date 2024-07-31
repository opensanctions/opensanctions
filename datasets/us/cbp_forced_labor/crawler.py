from lxml import html
import re
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, cast
from typing import List

REGEX_DATE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")


def convert_date(date_str: str) -> List[str]:
    """Convert various date formats to 'YYYY-MM-DD'."""
    dates = REGEX_DATE.findall(date_str)
    parsed = []
    formats = ["%m/%d/%Y"]  # 'MM/DD/YYYY' format
    for str in dates:
        parsed.extend(h.parse_date(str, formats))
    return parsed


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("Entities")  # Updated field to pull entity names
    schema = context.lookup_value("target_type", name)
    if schema is None:
        schema = "Company"
    entity = context.make(schema)
    entity.id = context.make_id(name)
    entity.add("name", name)

    # Add details to topics and sanctions
    status_element = row.get("Status")
    if status_element.lower() == "active":
        entity.add("topics", "sanction")
    else:
        entity.add("topics", "reg.warn")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)

    # Extract fields from the row
    listing_date = row.get("Date").text_content()
    listing_date_parsed = convert_date(listing_date)
    sanction.add("listingDate", listing_date_parsed)

    # Extract Source URL if exists in Status Notes or similar field
    status_notes = row.get("Status Notes").text_content()
    if "href" in status_notes:  # rudimentary check for an anchor tag
        anchors = html.fromstring(status_notes).findall(".//a")
        sanction.add("sourceUrl", [a.get("href") for a in anchors])

    context.emit(entity, target=True)
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                elt = cast(html.HtmlElement, el)
                headers.append(slugify(elt.text_content()))
            continue

        cells = row.findall("./td")
        if len(cells) == 0:
            continue  # Skip empty rows

        if len(headers) == len(cells):  # Ensure header and cell count match
            yield {hdr: c for hdr, c in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context):
    doc = context.fetch_html(context.data_url)  # Fetch the HTML document
    doc.make_links_absolute(context.data_url)  # Make links absolute if needed
    accordion_content = doc.xpath(
        "//div[@class='usa-accordion__content usa-prose']"
    )  # Adjusted to find the correct div

    # As there might be multiple accordions, we process each
    for accordion in accordion_content:
        table = accordion.find(".//table")  # Find the table within the accordion
        if table is not None:
            for row in parse_table(table):
                crawl_item(context, row)
