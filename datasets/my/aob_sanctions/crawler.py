from typing import Generator, Dict, cast
from lxml.etree import _Element
from normality import collapse_spaces, slugify

from zavod import Context, helpers as h


def parse_table(table: _Element) -> Generator[Dict[str, str], None, None]:
    """
    First we find the headers by searching the th tags, then we iterate over the tr tags (i.e. rows).

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = [th.text_content().strip() for th in table.findall(".//th")]
    for row in table.findall(".//tr")[1:]:
        if headers is None:
            headers = []
            for el in row.findall("./td"):
                headers.append(slugify(el.text_content()))
            continue

        cells = []
        for el in row.findall("./td"):
            cells.append(collapse_spaces(el.text_content()))

        # There are some incorrect rows with a smaller
        # number of values, we'll just ignore those
        if len(cells) != len(headers):
            continue

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    name_col = "Auditor" if "Auditor" in input_dict else "Parties Involved"
    date_col = (
        "Date of Action" if "Date of Action" in input_dict else "Date of AOB's Action"
    )
    description_col = (
        "Brief Description of Breach"
        if "Brief Description of Breach" in input_dict
        else "Brief Description of Misconduct"
    )
    reason_col = (
        "Nature of Breach"
        if "Nature of Breach" in input_dict
        else "Nature of Misconduct"
    )
    name = input_dict.pop(name_col)

    clean_names = context.lookup("clean_names", name)
    clean_names = cast("List[str]", clean_names.names)

    for clean_name in clean_names:
        if not clean_name:
            context.log.warning(f"Unable to parse name: {name}")
            return

        entity = context.make("Person")
        entity.id = context.make_id(clean_name)

        entity.add("name", clean_name)
        sanction = h.make_sanction(context, entity)

        sanction.add("description", input_dict.pop(description_col))
        sanction.add("reason", input_dict.pop(reason_col))

        sanction.add(
            "date",
            h.parse_date(input_dict.pop(date_col), formats=["%d %B %Y"]),
        )

        context.emit(entity, target=True)
        context.emit(sanction)

        context.audit_data(input_dict, ignore=["No.", "No", "Action Taken"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    # There is a table for each year
    for table in response.findall(".//table"):
        for item in parse_table(table):
            crawl_item(item, context)
