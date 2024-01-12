from zavod import Context
from typing import Dict, List, Optional
from pantomime.types import XLSX
import openpyxl
from normality import slugify
from zavod import helpers as h


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

    person.id = context.make_slug(id_number, prefix="uy-cedula")
    person.add("idNumber", id_number)
    person.add("name", person_name)

    position = h.make_position(context, role, country="uy", lang="spa")

    context.emit(person, target=True)
    context.emit(position)


def crawl(context: Context):
    path = context.fetch_resource("uy_pep_list.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])

    for row in rows:
        parse_sheet_row(context, row)
