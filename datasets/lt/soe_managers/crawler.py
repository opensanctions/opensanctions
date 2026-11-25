import re
from datetime import datetime
from typing import Any

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from rigour.mime.types import XLSX

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise

CEO_HEADERS = (
    "Įmonės pavadinimas",
    "Įmonės kodas",
    "Įmonės akcijų valdytojas/savininkas",
    "Vadovas",
    "Lytis",
    "Vadovo kadencijos pradžia",
    "Vadovo kadencijos pabaiga",
)
TERM_HEADERS = (
    "Eilė",
    "Lytis",
    "Einamosios kadencijos pradžia:",
    "Einamosios kadencijos pabaiga:",
    "Nario (-ės) pareigas eina nuo:",
    "Perrinktas (-a)",
)
COMPANY_HEADERS = (
    "Įmonės pavadinimas",
    "Įmonės kodas",
    "Įmonės akcijų valdytojas/savininkas",
    "Vardas, pavardė",
)
# Sheet name -> (English name of the body, header row). The CEO sheet is separate.
BOARD_SHEETS = {
    "Priežiūros valdyba": (
        "Board",
        COMPANY_HEADERS
        + (
            "Valdybos nario (-ės) kompetencijų sritys",
            "Valdybos nario (-ės) patirtis",
            "Eilė",
            "Pozicija valdyboje",
        )
        + TERM_HEADERS[1:],
    ),
    "Stebėtojų taryba": (
        "Supervisory Board",
        COMPANY_HEADERS
        + (
            "Stebėtojų tarybos nario (-ės) kompetencijų sritys",
            "Stebėtojų tarybos nario (-ės) patirtis",
            "Eilė",
            "Pozicija stebėtojų taryboje",
        )
        + TERM_HEADERS[1:],
    ),
    "Vykdančioji valdyba": (
        "Management Board",
        COMPANY_HEADERS + ("Eilė", "Pozicija valdyboje") + TERM_HEADERS[1:],
    ),
}
# "L.e.p." (laikinai einantis pareigas) marks an acting head.
ACTING_PREFIX = re.compile(r"^L\.\s*e\.\s*p\.\s*", re.IGNORECASE)
DATE = r"(\d{4}-\d{2}-\d{2})"


def cell_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if value is None:
        return None
    return " ".join(str(value).split()) or None


def read_sheet(
    sheet: Worksheet, headers: tuple[str, ...]
) -> list[tuple[dict[str, str | None], bool]]:
    """Return each data row with whether its name cell is bold, which marks a chair."""
    rows = sheet.iter_rows()
    found = tuple(cell_value(c.value) for c in next(rows))
    assert found == headers, (sheet.title, found)
    name_ix = 3
    records = []
    for cells in rows:
        values = [cell_value(c.value) for c in cells]
        assert not any(values[len(headers) :]), (sheet.title, values)
        record = dict(zip(headers, values))
        # Footer notes ("Pastaba: …", "Valdyba nėra išrinkta") have no company code.
        if record["Įmonės kodas"] is None:
            continue
        records.append((record, bool(cells[name_ix].font.b)))
    return records


def make_company(context: Context, row: dict[str, str | None]) -> Entity:
    code = row.pop("Įmonės kodas")
    company = context.make("Company")
    company.id = context.make_slug(code)
    company.add("name", row.pop("Įmonės pavadinimas"))
    company.add("registrationNumber", code)
    company.add("jurisdiction", "lt")
    company.add("topics", "gov.soe")
    return company


def make_person(context: Context, company: Entity, name: str | None) -> Entity:
    assert name is not None, company.id
    name = ACTING_PREFIX.sub("", name)
    person = context.make("Person")
    person.id = context.make_id(company.id, name)
    person.add("name", name)
    # 'country' rather than 'citizenship': board members include foreign nationals.
    person.add("country", "lt")
    return person


def emit_occupancy(
    context: Context,
    person: Entity,
    company: Entity,
    role: str,
    start_date: str | None,
    end_date: str | None,
    status: OccupancyStatus | None,
) -> None:
    position = h.make_position(
        context,
        name=f"{role} of {company.first('name')}",
        country="lt",
        topics=["gov.soe"],
        organization=company,
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        status=status,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(position)
    context.emit(company)
    context.emit(person)


def crawl_ceo(context: Context, row: dict[str, str | None]) -> None:
    company = make_company(context, row)
    person = make_person(context, company, row.pop("Vadovas"))
    person.add("gender", row.pop("Lytis"))
    start_date = row.pop("Vadovo kadencijos pradžia")
    assert start_date is not None, person.id
    # Acting heads have no term, only "pareigas eina nuo <date>".
    acting = re.fullmatch(f"pareigas eina nuo {DATE}", start_date)
    if acting is not None:
        start_date = acting.group(1)
    emit_occupancy(
        context,
        person,
        company,
        "Chief Executive",
        start_date,
        row.pop("Vadovo kadencijos pabaiga"),
        # Listed heads are in office, even when the scheduled term end has passed.
        OccupancyStatus.CURRENT,
    )
    context.audit_data(row, ignore=["Įmonės akcijų valdytojas/savininkas"])


def crawl_board_member(
    context: Context, body: str, row: dict[str, str | None], is_chair: bool
) -> None:
    company = make_company(context, row)
    name = row.pop("Vardas, pavardė")
    if name is None:
        context.log.info("No board elected", body=body, company=company.id)
        return
    person = make_person(context, company, name)
    person.add("gender", row.pop("Lytis"))
    for header in (
        "Valdybos nario (-ės) patirtis",
        "Stebėtojų tarybos nario (-ės) patirtis",
    ):
        person.add("biography", row.pop(header, None), lang="lit")

    start_date = row.pop("Einamosios kadencijos pradžia:")
    serving_since = row.pop("Nario (-ės) pareigas eina nuo:")
    if serving_since is not None:
        earlier = re.fullmatch(
            f"Valdybos nar(?:io|ės) pareigas taip pat ėjo nuo {DATE} iki {DATE}",
            serving_since,
        )
        if earlier is not None:
            emit_occupancy(
                context,
                person,
                company,
                f"Member of the {body}",
                earlier.group(1),
                earlier.group(2),
                None,
            )
        else:
            # A date: the start of the first of consecutive terms.
            start_date = serving_since
    role = f"Chair of the {body}" if is_chair else f"Member of the {body}"
    emit_occupancy(
        context,
        person,
        company,
        role,
        start_date,
        row.pop("Einamosios kadencijos pabaiga:"),
        OccupancyStatus.CURRENT,
    )
    context.audit_data(
        row,
        ignore=[
            "Įmonės akcijų valdytojas/savininkas",
            # Order of the members within the board.
            "Eilė",
            # Seat type (independent, civil servant, employee representative, …):
            # every member of the body is covered alike.
            "Pozicija valdyboje",
            "Pozicija stebėtojų taryboje",
            "Valdybos nario (-ės) kompetencijų sritys",
            "Stebėtojų tarybos nario (-ės) kompetencijų sritys",
            # Date of re-election; the start of the first term is used instead.
            "Perrinktas (-a)",
        ],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    url = h.xpath_string(
        doc, "//a[normalize-space()='VVĮ vadovų ir KO narių sąrašas']/@href"
    )
    path = context.fetch_resource("managers.xlsx", url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    # Not read-only: the bold font marking chairs is only loaded in full mode.
    workbook = load_workbook(path, data_only=True)
    assert set(workbook.sheetnames) == {"Vadovybė", *BOARD_SHEETS}, workbook.sheetnames
    for row, _ in read_sheet(workbook["Vadovybė"], CEO_HEADERS):
        crawl_ceo(context, row)
    for sheet_name, (body, headers) in BOARD_SHEETS.items():
        for row, is_chair in read_sheet(workbook[sheet_name], headers):
            crawl_board_member(context, body, row, is_chair)
