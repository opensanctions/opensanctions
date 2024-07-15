from lxml import html
import re
from datetime import datetime
from zavod import Context, helpers as h
from slugify import slugify
from typing import Dict, Generator


def convert_date(date_str: str) -> str or None:
    """Convert various date formats to 'YYYY-MM-DD'."""
    # Regular expression to find dates in the format 'MM/DD/YYYY'
    date_pattern = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
    match = date_pattern.search(date_str)
    if match:
        date_str = match.group(1)  # Extract the first matching date

    formats = [
        "%m/%d/%Y",  # 'MM/DD/YYYY' format
        "%B %d, %Y",  # 'Month DD, YYYY' format
        "%d-%b-%y",  # 'DD-MMM-YY' format
    ]

    for fmt in formats:
        try:
            # Parse the date string and return in 'YYYY-MM-DD' format
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    print(row)
    name = row.pop("company")
    schema = context.lookup_value("target_type", name, default="Company")
    entity = context.make(schema)
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("topics", "sanction")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)
    sanction.add("country", "us")
    finding_date = row.get("finding", "")
    nprm_date = row.get("notice_of_proposed_rulemaking", "")
    listing_date = finding_date if finding_date else nprm_date
    listing_date = convert_date(listing_date)
    sanction.add("listingDate", listing_date)
    final_rule_date = row.get("final_rule", "")
    if final_rule_date != "---":
        sanction.add("startDate", convert_date(final_rule_date))

    rescinded_date = row.get("rescinded", "")
    if rescinded_date != "---":
        sanction.add("endDate", convert_date(rescinded_date))

    # Emit the entity and the sanction
    context.emit(entity, target=True)
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [
                slugify(el.text_content(), separator="_")
                if el.text_content().strip()
                else "company"
                for el in row.findall("./th")
            ]
            continue
        cells = [el.text_content().strip() for el in row.findall("./td")]
        if len(cells) != len(headers):
            continue
        # yield {header: cell for header, cell in zip(headers, cells)}
        row_dict = {header: cell for header, cell in zip(headers, cells)}
        row_dict["match"] = row_dict.get(
            "match", ""
        ).strip()  # Ensure match value is stripped of whitespace
        yield row_dict


# Main crawl function to fetch and process data.
def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    table = doc.get_element_by_id("special-measures-table")
    if table is not None:
        for row in parse_table(table):
            crawl_item(context, row)
    else:
        context.log.error("Table with id 'special-measures-table' not found.")
