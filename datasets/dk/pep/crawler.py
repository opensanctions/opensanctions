import openpyxl
from openpyxl import load_workbook
from typing import Generator
from pantomime.types import XLSX
from normality import stringify, slugify
from datetime import datetime

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

RESOURCEs = [
    "PEP_listen.xlsx",
    "PEP_listen_faeroeerne.xlsx",
    "PEP_listen_groenland.xlsx",
]

BASE_URL = "https://www.finanstilsynet.dk/-/media/Tal-og-fakta/PEP/"


def header_names(cells):
    headers = []
    for idx, cell in enumerate(cells):
        if cell is None:
            cell = f"column_{idx}"
        headers.append(slugify(cell, "_").lower())
    return headers


def parse_old_pep(
    sheet: openpyxl.worksheet.worksheet.Worksheet,
) -> Generator[dict, None, None]:
    """This function parses the old PEP list, which is in the format of a table in the second sheet of each Excel file.

    Args:
        sheet (openpyxl.worksheet.worksheet.Worksheet): The sheet to parse.

    Yields:
        Generator[dict, None, None]: Each row of the table as a dictionary.
    """

    headers = None
    for idx, row in enumerate(sheet.rows):
        cells = [c.value for c in row]
        if headers is None:
            headers = header_names(cells)
            continue
        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            value = stringify(value)
            if value is not None:
                record[header] = value
        if len(record) == 0:
            continue
        yield record


def parse_current_pep(
    sheet: openpyxl.worksheet.worksheet.Worksheet, context: Context
) -> Generator[dict, None, None]:
    """This function parses the current PEP list, which is in the format of multiple tables in the first sheet of the Excel file.

    Args:
        sheet (openpyxl.worksheet.worksheet.Worksheet): The sheet to parse.

    Yields:
        Generator[dict, None, None]: Each row of the table as a dictionary.
    """
    for idx, row in enumerate(sheet.rows):
        # First row is just the date and name of the dataset
        # Second row is the header
        if idx in [0, 1]:
            continue

        # Every row can be either:
        # - Name of the organization
        # - Data of the PEP

        # If there are no values in the row, then it's an empty row
        if all([c.value is None for c in row]):
            pass

        # If there is only one not null value in the row and it's the first, then it's the name of the list
        elif sum([c.value is not None for c in row]) == 1 and row[0].value is not None:
            list_name = row[0].value

        # If at least one of the 2nd, 3rd, 4th, 5th and 6th columns have values, then it's a PEP data
        elif any([c.value is not None for c in row[1:6]]):
            # Meaning "No boards"
            if row[1].value == "Ingen styrelser etc.":
                continue

            yield {
                "last-name": row[1].value,
                "first-name": row[2].value,
                "position": row[3].value,
                "birth-date": row[4].value,
                "start-date": row[5].value,
                "list-name": list_name,
            }

        else:
            context.log.warning(f"Couldn't parse row: {[c.value for c in row]}")


def crawl_current_pep_item(input_dict: dict, context: Context):
    first_name = input_dict.pop("first-name")
    last_name = input_dict.pop("last-name")

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    entity.add(
        "birthDate", h.parse_date(input_dict.pop("birth-date"), formats=["%d.%m.%Y"])
    )

    position = h.make_position(
        context, input_dict.pop("position"), country="dk", lang="dk"
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        start_date=h.parse_date(input_dict.pop("start-date"), formats=["%d.%m.%Y"])[0],
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(entity, target=True)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(input_dict, ignore=["list-name"])


def crawl_old_pep_item(input_dict: dict, context: Context):
    last_name = input_dict.pop("efternavn")
    first_name = input_dict.pop("fornavn")

    position_col = "stilling" if "stilling" in input_dict else "stillingsbetegnelse"

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    if "fodselsdato" in input_dict:
        birth_date = input_dict.pop("fodselsdato")
        entity.add("birthDate", h.parse_date(birth_date, formats=["%d.%m.%Y"]))

    position = h.make_position(
        context, input_dict.pop(position_col), country="dk", lang="dk"
    )

    occupation = h.make_occupancy(
        context,
        entity,
        position,
        True,
        end_date=h.parse_date(
            input_dict.pop("fjernet_fra_pep_listen_dato"), formats=["%d.%m.%Y"]
        )[0],
        categorisation=categorise(context, position, is_pep=True),
    )

    if occupation:
        context.emit(entity, target=True)
        context.emit(position)
        context.emit(occupation)

    context.audit_data(input_dict)


def crawl(context: Context):
    for name in RESOURCEs:
        path = context.fetch_resource(name, BASE_URL + name)
        context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

        wb = load_workbook(path, read_only=True)

        # Crawl old PEP list
        for item in parse_old_pep(wb["Tidligere PEP'ere"]):
            crawl_old_pep_item(item, context)

        # Crawl current PEP list
        for item in parse_current_pep(wb["Nuværende PEP'ere"], context):
            crawl_current_pep_item(item, context)
