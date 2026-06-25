from typing import Iterator
from lxml.etree import tostring
from normality import collapse_spaces, slugify

from zavod import Context, helpers as h
from zavod.util import Element

SPLITTERS = [
    "(",
    "former partner of",
    "partner of",
    "Partner of",
    " of ",
]

PROVISIONS_SPLITS = ["Appeal", "Judicial Review"]


def parse_table(table: Element) -> Iterator[dict[str, str | None]]:
    """
    First we find the headers by searching the th tags, then we iterate over the tr tags (i.e. rows).

    Returns:
        An iterator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values (str or None).
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = [h.element_text(th) for th in table.findall(".//th")]
    # Rowspan state for last column. Value may be used by next row if span > 1.
    rowspan_value: str | None = None
    rowspan = 1
    for row in table.findall(".//tr")[1:]:
        if headers is None:
            headers = []
            for el in row.findall("./td"):
                headers.append(slugify(h.element_text(el)))
            continue

        cells: list[str | None] = []
        for idx, el in enumerate(row.findall("./td")):
            value = collapse_spaces(h.element_text(el))
            cells.append(value)

            # handle rowspan for last column
            if idx == len(headers) - 1:
                if el.get("rowspan") is None:
                    rowspan_value = None
                    rowspan = 1
                else:
                    assert rowspan == 1, (
                        "Can't start new rowspan before previous one finished",
                        rowspan,
                        tostring(row),
                    )
                    rowspan_value = value
                    rowspan_str = el.get("rowspan")
                    assert rowspan_str is not None
                    rowspan = int(rowspan_str)

        if len(cells) == len(headers) - 1:
            assert rowspan > 1, (
                "Can't use rowspan value when we're not in its span",
                rowspan,
            )
            cells.append(rowspan_value)
            rowspan -= 1
        assert len(cells) == len(headers), (len(cells), len(headers))

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(context: Context, input_dict: dict[str, str | None]) -> None:
    dict_keys = list(input_dict.keys())

    for column in dict_keys:
        new_column = context.lookup_value("columns", column)
        if new_column is not None:
            input_dict[new_column] = input_dict.pop(column)

    parties = input_dict.pop("parties")
    assert parties is not None, "Expected non-None parties value"
    # The abbreviation a/l stands for anak lelaki, which means "son of" (s/o) in Malay
    split_parties = h.multi_split(parties, SPLITTERS)
    clean_name: str | None
    if len(split_parties) > 1:
        clean_name = split_parties[0]
    else:
        clean_name = context.lookup_value("name", parties)
    if clean_name is None:
        context.log.warning(f"Unable to parse parties: {parties}")
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(parties)

    entity.add("name", clean_name)
    entity.add("topics", "reg.warn")
    entity.add("country", "my")
    entity.add("description", parties)

    sanction = h.make_sanction(context, entity)
    sanction.add("description", input_dict.pop("description"))
    sanction.add("reason", input_dict.pop("reason"))
    sanction.add(
        "provisions", h.multi_split(input_dict.pop("Action Taken"), PROVISIONS_SPLITS)
    )
    h.apply_date(sanction, "startDate", input_dict.pop("date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(input_dict, ignore=["No.", "No"])


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url)

    # There is a table for each year
    for table in response.findall(".//table"):
        for item in parse_table(table):
            crawl_item(context, item)
