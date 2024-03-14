from typing import Generator, Dict, Tuple

from normality import collapse_spaces
from zavod import Context, helpers as h

HEADERS = ["Person Name", "ID", "Date of Freezing"]

def parse_table(
    table,
) -> Generator[Dict[str, str | Tuple[str]], None, None]:
    """
    The first row of the table represent the headers, but we're not going to
    try and parse colspan and rowspan.

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """

    for _, row in enumerate(table.findall(".//tr")[1:]):
        cells = [collapse_spaces(cell.text_content()) for cell in row.findall("./td")]
        assert len(cells) == len(HEADERS), cells

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(HEADERS, cells, strict=True)}

def crawl_item(input_dict: dict, context: Context):
    entity = context.make("Person")

    entity.id = context.make_slug(input_dict.pop("ID"))

    # We are going to split using the dot symbol used to represent the start of a new name
    # Then we will strip leading and trailling spaces
    # Finally, we will remove the information contained in the brackets, because they are not relevant
    names = [name.strip() for name in input_dict.pop("Person Name").split("• ") if name.strip()]
    names = [h.remove_bracketed(name) for name in names]

    for name in names:
        entity.add("name", name)

    sanction = h.make_sanction(context, entity)
    sanction.add("date", h.parse_date(input_dict.pop("Date of Freezing"), formats=["%d/%m/%Y"]))

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(input_dict)

def crawl(context: Context):


    response = context.fetch_html(context.data_url)

    table = response.find(".//table")

    for item in parse_table(table):
        crawl_item(item, context)
