import re
from collections.abc import Iterator

from normality import slugify

from zavod import Context, helpers as h
from zavod.util import Element


REGEX_DATE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")


def convert_date(date_str: str) -> list[str]:
    """Convert various date formats to 'YYYY-MM-DD'."""
    dates = REGEX_DATE.findall(date_str)
    return dates


def crawl_item(context: Context, row: dict[str, Element]) -> None:
    # Create the entity based on the schema
    name = h.element_text(row.pop("name"))
    schema = context.lookup_value("target_type", name)
    if schema is None:
        schema = "Company"
    entity = context.make(schema)
    entity.id = context.make_id(name)
    entity.add("name", name)
    # Adjust the topic based on the presence of "final rule"
    final_rule = h.element_text(row["final-rule"]).lower()
    rescinded_date = h.element_text(row["rescinded"])
    if (
        final_rule
        and final_rule != "---"
        and (not rescinded_date or rescinded_date == "---")
    ):
        entity.add("topics", "sanction")
    else:
        entity.add("topics", "reg.warn")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)

    # Extract PDF links
    source_urls = h.xpath_strings(row["finding"], ".//a/@href")
    source_urls.extend(
        h.xpath_strings(row["notice-of-proposed-rulemaking"], ".//a/@href")
    )
    source_urls.extend(h.xpath_strings(row["rescinded"], ".//a/@href"))
    source_urls.extend(h.xpath_strings(row["final-rule"], ".//a/@href"))
    sanction.add("sourceUrl", source_urls)

    finding_date = h.element_text(row["finding"])
    nprm_date = h.element_text(row["notice-of-proposed-rulemaking"])
    listing_date = finding_date if finding_date else nprm_date
    for date in convert_date(listing_date):
        h.apply_date(sanction, "listingDate", date)

    final_rule_date = h.element_text(row["final-rule"])
    if final_rule_date != "---":
        for date in convert_date(final_rule_date):
            h.apply_date(sanction, "startDate", date)

    if rescinded_date != "---" and rescinded_date != "":
        for date in convert_date(rescinded_date):
            h.apply_date(sanction, "endDate", date)
        context.emit(entity)
    else:
        context.emit(entity)

    # Emit the sanction
    context.emit(sanction)


# Parse the table and yield rows as dictionaries.
def parse_table(table: Element) -> Iterator[dict[str, Element]]:
    headers: list[str] | None = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for idx, el in enumerate(row.findall("./th")):
                slug = slugify(h.element_text(el))
                if idx == 0:
                    # The first column has no header in the source; it holds the name.
                    assert slug is None, slug
                    slug = "name"
                assert slug is not None, h.element_text(el)
                headers.append(slug)
            # no duplicate column headers
            assert len(set(headers)) == len(headers), headers
            continue

        cells = row.findall("./td")
        if len(cells) == 1:
            continue
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    for table in h.xpath_elements(doc, '//table[@id="special-measures-table"]'):
        for row in parse_table(table):
            crawl_item(context, row)
