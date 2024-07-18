from lxml import html
import re
from zavod import Context, helpers as h
from normality import slugify
from typing import Dict, Generator, cast
from typing import List


def convert_date(date_str: str) -> List[str]:
    """Convert various date formats to 'YYYY-MM-DD'."""
    # Regular expression to find dates in the format 'MM/DD/YYYY'
    date_pattern = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
    date_pattern.search(date_str)

    formats = [
        "%m/%d/%Y",  # 'MM/DD/YYYY' format
        "%B %d, %Y",  # 'Month DD, YYYY' format
        "%d-%b-%y",  # 'DD-MMM-YY' format
    ]
    date = h.parse_date(date_str, formats)
    return date


def crawl_item(context: Context, row: Dict[str, str]):
    # Create the entity based on the schema
    name = row.pop("name").text_content()
    schema = context.lookup_value("target_type", name)
    if schema is None:
        schema = "Company"
    entity = context.make(schema)
    entity.id = context.make_id(name)
    entity.add("name", name)
    # Adjust the topic based on the presence of "final rule"
    final_rule = row.get("final_rule", "").strip().lower()
    if final_rule and final_rule != "---" and final_rule != "":
        entity.add("topics", "sanction")
    else:
        entity.add("topics", "reg.warn")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)

    # Extract PDF links
    sanction.add(
        "sourceUrl", [e.get("href") for e in row.get("finding").findall(".//a")]
    )
    sanction.add(
        "sourceUrl",
        [
            e.get("href")
            for e in row.get("notice-of-proposed-rulemaking").findall(".//a")
        ],
    )
    sanction.add(
        "sourceUrl", [e.get("href") for e in row.get("rescinded").findall(".//a")]
    )
    sanction.add(
        "sourceUrl", [e.get("href") for e in row.get("final-rule").findall(".//a")]
    )

    finding_date = row.get("finding").text_content()
    nprm_date = row.get("notice-of-proposed-rulemaking").text_content()
    listing_date = finding_date if finding_date else nprm_date
    listing_date = convert_date(listing_date)
    sanction.add("listingDate", listing_date)

    final_rule_date = row.get("final-rule").text_content()
    if final_rule_date != "---":
        sanction.add("startDate", convert_date(final_rule_date))

    rescinded_date = row.get("rescinded").text_content()
    if rescinded_date != "---":
        sanction.add("endDate", convert_date(rescinded_date))
        context.emit(entity, target=True)
    else:
        context.emit(entity, target=False)

    # Emit the entity and the sanction
    # context.emit(entity)
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: html.HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(html.HtmlElement, el)
                headers.append(slugify(eltree.text_content()))
            assert headers[0] is None, headers
            assert len(set(headers)) == len(headers), headers
            headers[0] = "name"
            continue

        cells = row.findall("./td")  # <--- edited here
        if len(cells) == 1:
            continue
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    table = doc.get_element_by_id("special-measures-table")
    if table is not None:
        for row in parse_table(table):
            crawl_item(context, row)
