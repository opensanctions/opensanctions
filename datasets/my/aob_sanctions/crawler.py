from typing import Generator, Dict, cast
from lxml.etree import _Element
from normality import collapse_spaces, slugify

from zavod import Context, helpers as h

SPLITTERS = [
    "(",
    "former partner of",
    "partner of",
    "Partner of",
    " of ",
]


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

    dict_keys = list(input_dict.keys())

    for column in dict_keys:
        new_column = context.lookup_value("columns", column)
        if new_column is not None:
            input_dict[new_column] = input_dict.pop(column)

    parties = input_dict.pop("parties")
    # The abbreviation a/l stands for anak lelaki, which means "son of" (s/o) in Malay
    split_parties = h.multi_split(parties, SPLITTERS)
    if len(split_parties) > 1:
        clean_name = split_parties[0]
    else:
        clean_name = context.lookup_value("name", parties)
    if clean_name is None:
        context.log.warning(f"Unable to parse parties: {parties}")
        return

    entity = context.make("Person")
    entity.id = context.make_id(clean_name)

    entity.add("name", clean_name)
    entity.add("topics", "reg.warn")
    entity.add("country", "my")
    entity.add("description", parties)
    
    sanction = h.make_sanction(context, entity)
    sanction.add("description", input_dict.pop("description"))
    sanction.add("reason", input_dict.pop("reason"))
    sanction.add("provisions", input_dict.pop("Action Taken"))

    sanction.add(
        "date",
        h.parse_date(input_dict.pop("date"), formats=["%d %B %Y"]),
    )

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict, ignore=["No.", "No"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    # There is a table for each year
    for table in response.findall(".//table"):
        for item in parse_table(table):
            crawl_item(item, context)
