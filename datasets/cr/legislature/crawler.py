import re
from urllib.parse import quote

from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

SITE = "https://www.asamblea.go.cr"
# SharePoint document library folder holding the monthly deputy salary reports. The
# crawler lists it via the SharePoint REST API (context.data_url) and picks the latest
# file; if the folder is moved or emptied the listing fails loudly rather than emitting a
# stale roster.
SALARY_FOLDER = "/pa/datosabiertos/Documentos compartidos/SalarioDiputados"
# Files are named e.g. "2026-05-Salario Diputados.xlsx"; the YYYY-MM prefix sorts
# chronologically, so the lexicographically greatest matching name is the newest report.
FILE_RE = re.compile(r"^(\d{4}-\d{2})-Salario Diputados\.xlsx$")

IGNORE_COLUMNS = [
    "mes",
    "ano",
    "dieta",
    "gastos_de_representacion",
    "salario_bruto",
    "column_8",
    "column_9",
]


def latest_report_url(context: Context) -> str:
    """Return the absolute URL of the most recent monthly salary report."""
    data = context.fetch_json(
        context.data_url,
        params={"$select": "Name,ServerRelativeUrl"},
        headers={"Accept": "application/json;odata=verbose"},
        cache_days=1,
    )
    latest_key: str | None = None
    latest_path: str | None = None
    for entry in data["d"]["results"]:
        match = FILE_RE.match(entry["Name"])
        if match is None:
            continue
        key = match.group(1)
        if latest_key is None or key > latest_key:
            latest_key = key
            latest_path = entry["ServerRelativeUrl"]
    if latest_path is None:
        raise ValueError(f"No monthly salary report found in {SALARY_FOLDER}")
    # ServerRelativeUrl contains spaces and accented characters; percent-encode the path.
    return SITE + quote(latest_path)


def crawl_deputy(
    context: Context,
    row: dict[str, str | None],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = row.pop("nombre_completo")
    fraccion = row.pop("fraccion")
    clase = row.pop("clase_de_puesto")
    # Trailing blank rows and the footnote row ("Nota: ...") carry no parliamentary group;
    # only rows with a fracción are deputies.
    if name is None or fraccion is None:
        return
    # Every real row should be a deputy; fail loudly if the role column is unexpected.
    if clase not in ("Diputado", "Diputada"):
        raise ValueError(f"Unexpected role {clase!r} for {name!r}")

    person = context.make("Person")
    person.id = context.make_id("cr-diputado", name)
    person.add("name", name)
    # Deputies must be Costa Rican citizens — by birth or naturalised with ten years'
    # residence (Political Constitution of Costa Rica, Art. 108).
    # http://www.pgrweb.go.cr/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?param1=NRTC&nValor1=1&nValor2=871&strTipM=TC
    person.add("citizenship", "cr")
    person.add("political", fraccion)

    # No start/end dates are published in this report and it is refreshed monthly, so it
    # is treated as a live roster: a listed deputy is currently in office.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_COLUMNS)


def crawl(context: Context) -> None:
    url = latest_report_url(context)
    path = context.fetch_resource("diputados.xlsx", url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook = load_workbook(path, read_only=True)
    sheet = workbook.worksheets[0]

    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of Costa Rica",
        country="cr",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328617",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in h.parse_xlsx_sheet(context, sheet):
        crawl_deputy(context, row, position, categorisation)
