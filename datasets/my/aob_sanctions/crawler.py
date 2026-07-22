from copy import deepcopy

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


def expand_rowspans(table: Element) -> None:
    """Replace cells that span multiple rows with a copy in each row they cover.

    `h.parse_html_table` expects every data row to have exactly one cell per
    column. The source tables use `rowspan` (only on the date column, in
    practice) to share a value across consecutive rows, leaving those rows one
    cell short. Inserting copies first lets the standard helper parse the table.
    """
    ncols = len(table.findall(".//th"))
    assert ncols > 0, "Table has no header cells"

    # carry[col] = (remaining_rows, element) for cells still spanning downward.
    carry: dict[int, tuple[int, Element]] = {}
    for row in table.findall(".//tr"):
        cells = row.findall("./td")
        if not cells:  # header row (th only) or structural empty row
            continue

        source = iter(cells)
        expanded: list[Element] = []
        for col in range(ncols):
            if col in carry:
                remaining, spanned = carry.pop(col)
                if remaining > 1:
                    carry[col] = (remaining - 1, spanned)
                expanded.append(deepcopy(spanned))
            elif (cell := next(source, None)) is not None:
                expanded.append(cell)
                if (rowspan := int(cell.get("rowspan") or 1)) > 1:
                    carry[col] = (rowspan - 1, cell)
        row[:] = expanded


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
        expand_rowspans(table)
        for row in h.parse_html_table(table, slugify_headers=False):
            item = h.cells_to_str(row)
            # Some tables have a trailing row with all cells empty.
            if all(value is None for value in item.values()):
                continue
            crawl_item(context, item)
