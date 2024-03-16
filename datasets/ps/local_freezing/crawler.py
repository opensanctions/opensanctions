from typing import Generator, Dict

from normality import collapse_spaces, slugify
from zavod import Context, helpers as h


def parse_table(table) -> Generator[Dict[str, str], None, None]:
    """
    The first row of the table represent the headers, but we're not going to
    try and parse colspan and rowspan.

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./td"):
                headers.append(slugify(el.text_content()))
            continue

        cells = []
        for el in row.findall("./td"):
            cells.append(collapse_spaces(el.text_content()))

        assert len(cells) == len(headers)

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    entity = context.make("Person")

    id_ = input_dict.pop("id")

    entity.id = context.make_slug(id_)

    entity.add("idNumber", id_)
    entity.add("country", "ps")

    # We are going to split using the dot symbol used to represent the start of a new name
    # Then we will strip leading and trailling spaces
    # Finally, we will remove the information contained in the brackets, because they are not relevant
    names = [
        name.strip()
        for name in input_dict.pop("person-name").split("• ")
        if name.strip()
    ]
    names = [h.remove_bracketed(name) for name in names]

    for name in names:
        entity.add("name", name)

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "date", h.parse_date(input_dict.pop("date-of-freezing"), formats=["%d/%m/%Y"])
    )
    sanction.add(
        "program",
        "Decree No. (14) of 2015 Concerning the Enforcement of Security Council Resolutions",
    )

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    table = response.find(".//table")

    for item in parse_table(table):
        crawl_item(item, context)
