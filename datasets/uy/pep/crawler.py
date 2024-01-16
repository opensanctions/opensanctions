from zavod import Context
from typing import Dict, List, Optional
from pantomime.types import XLSX
import openpyxl
from normality import slugify
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus, categorise


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]

        if headers is None and all(cells):
            headers = [slugify(h) for h in cells]
            continue

        if headers:
            row = dict(zip(headers, cells))
            yield row


def parse_sheet_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    id_number = row.pop("c-i")
    person_name = row.pop("nombre")
    role = row.pop("cargo")
    organization_name = row.pop("organismo")

    person.id = context.make_slug(id_number, prefix="uy-cedula")
    person.add("idNumber", id_number)
    person.add("name", person_name)

    position = h.make_position(
        context, f"{role}, {organization_name}", country="uy", lang="spa"
    )

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        status=OccupancyStatus.UNKNOWN,
        categorisation=categorisation,
    )

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("uy_pep_list.xlsx", context.data_url)

    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])

    for row in rows:
        parse_sheet_row(context, row)
