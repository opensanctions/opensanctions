from lxml import html
import re
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator

BASE_URL = "https://www.fincen.gov"


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
    return h.parse_date(date_str, formats)


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("company")
    schema = context.lookup_value("target_type", name)
    if schema is None:
        schema = "Company"
    entity = context.make(schema)
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "sanction")

    # Extract PDF links
    pdf_link_finding = row.get("pdf_link_finding", None)
    pdf_link_nprm = row.get("pdf_link_nprm", None)
    pdf_link_final_rule = row.get("pdf_link_final_rule", None)
    pdf_link_rescinded = row.get("pdf_link_rescinded", None)

    # Convert relative URLs to full URLs
    if pdf_link_finding and pdf_link_finding.startswith("/"):
        pdf_link_finding = BASE_URL + pdf_link_finding
    if pdf_link_nprm and pdf_link_nprm.startswith("/"):
        pdf_link_nprm = BASE_URL + pdf_link_nprm
    if pdf_link_final_rule and pdf_link_final_rule.startswith("/"):
        pdf_link_final_rule = BASE_URL + pdf_link_final_rule
    if pdf_link_rescinded and pdf_link_rescinded.startswith("/"):
        pdf_link_rescinded = BASE_URL + pdf_link_rescinded

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

    # Add description with the PDF link
    if pdf_link_finding:
        sanction.add("description", pdf_link_finding)  # Finding PDF link
    if pdf_link_nprm:
        sanction.add("description", pdf_link_nprm)  # NPRM PDF link
    if pdf_link_final_rule:
        sanction.add("description", pdf_link_final_rule)  # Final Rule PDF link
    if pdf_link_rescinded:
        sanction.add("description", pdf_link_rescinded)  # Rescinded PDF link

    # Emit the entity and the sanction
    context.emit(entity, target=True)
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [
                slugify(el.text_content(), sep="_")
                if el.text_content().strip()
                else "company"
                for el in row.findall("./th")
            ]
            headers.extend(
                [
                    "pdf_link_finding",
                    "pdf_link_nprm",
                    "pdf_link_final_rule",
                    "pdf_link_rescinded",
                ]
            )
            continue
        cells = [el.text_content().strip() for el in row.findall("./td")]

        # Extract PDF links from the relevant columns
        pdf_link_finding = None
        pdf_link_nprm = None
        pdf_link_final_rule = None
        pdf_link_rescinded = None

        pdf_links = row.findall(".//a[@href]")

        # Assuming the order of links in the row corresponds to the columns: Finding, NPRM, and Final Rule.
        for i, pdf_link_element in enumerate(pdf_links):
            pdf_link = pdf_link_element.get("href")
            if pdf_link.endswith(".pdf"):
                if i == 0:
                    pdf_link_finding = pdf_link
                elif i == 1:
                    pdf_link_nprm = pdf_link
                elif i == 2:
                    pdf_link_final_rule = pdf_link
                elif i == 3:
                    pdf_link_rescinded = pdf_link

        if (
            len(cells) != len(headers) - 4
        ):  # Minus four because we added the 'pdf_link_*' headers
            continue
        cells.extend(
            [pdf_link_finding, pdf_link_nprm, pdf_link_final_rule, pdf_link_rescinded]
        )
        yield {header: cell for header, cell in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    table = doc.get_element_by_id("special-measures-table")
    if table is not None:
        for row in parse_table(table):
            crawl_item(context, row)
    else:
        context.log.error("Table with id 'special-measures-table' not found.")
