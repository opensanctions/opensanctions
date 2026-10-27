from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise


def parse_sheet_row(context: Context, row: dict[str, str | None]) -> None:
    person = context.make("Person")

    id_number = row.pop("c_i")
    person_name = row.pop("nombre")
    role = row.pop("cargo")
    organization_name = row.pop("organismo")

    person.id = context.make_slug(id_number, prefix="uy-cedula")
    person.add("idNumber", id_number)
    person.add("name", person_name)
    # list includes international org officials, so not all require citizenship
    person.add("country", "uy")

    position = h.make_position(
        context,
        f"{role}, {organization_name}",
        country="uy",
        lang="spa",
        translate_name=True,
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

    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)
    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("uy_pep_list.xlsx", context.data_url)

    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = CalamineWorkbook.from_path(path)
    assert workbook.sheet_names == ["lista_pep"], workbook.sheet_names
    sheet = workbook.get_sheet_by_name("lista_pep")
    for row in h.parse_xlsx_sheet(context, sheet, skiprows=4):
        parse_sheet_row(context, row)
