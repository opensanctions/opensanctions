import re
from datetime import datetime
from io import IOBase
from typing import Optional

from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTTextBoxHorizontal

from zavod import Context, Entity
from zavod import helpers as h

MONTHS_DE = {
    "Januar": 1,
    "Jänner": 1,
    "Februar": 2,
    "März": 3,
    "April": 4,
    "Mai": 5,
    "Juni": 6,
    "Juli": 7,
    "August": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Dezember": 12,
}


COUNTRY_CODES = {
    "A": "at",  # Austria
    "D": "de",  # Germany
    "F": "fr",  # France
    "I": "it",  # Italy
    "SL": "si",  # Slovenia
}


ADDRESS_FIXES = {
    "Alte Tiefenaustrasse 6, 3050 Bern": "Alte Tiefenaustrasse 6, CH‐3050 Bern",
    "Bachwisenstrasse 7c, 9200 CH-Gossau SG": "Bachwisenstrasse 7c, CH‐9200 Gossau SG",
    "Industrie Allmend 31, 4629 Fulenbach": "Industrie Allmend 31, CH‐4629 Fulenbach",
    "Hagenholzstrasse 81a, 8050 CH‐Zürich": "Hagenholzstrasse 81a, CH‐8050 Zürich",
    "Tiefenackerstrasse 59 CH‐9450 Altstätten": "Tiefenackerstrasse 59, CH‐9450 Altstätten",
    "Unterdorfstrasse 94, 9443 Widnau": "Unterdorfstrasse 94, CH‐9443 Widnau",
}


def parse_data_time(rows: list[str]) -> Optional[datetime]:
    for row in rows:
        if m := re.match(r"^Stand: .+ (\d+)\. ([A-Za-zä]+) (\d{4})$", row[0]):
            return datetime(
                year=int(m.group(3)), month=MONTHS_DE[m.group(2)], day=int(m.group(1))
            )
    return None


def parse_address(context: Context, addr: str) -> Optional[Entity]:
    addr = ADDRESS_FIXES.get(addr, addr)
    parts = [p.strip() for p in addr.split(",")]
    street = parts[0]
    country_code, postal_code, city = None, None, None
    if m := re.match(r"(A|D|F|I|[A-Z]{2})[‐ ]\s*([\d‐\-]+) (.+)", parts[-1]):
        country_code = COUNTRY_CODES.get(m.group(1), m.group(1).lower())
        postal_code, city = m.group(2), m.group(3)
    if not country_code or not city:
        context.log.warn(f'Cannot parse address "{addr}"')
    return h.make_address(
        context=context,
        full=addr,
        street=street,
        postal_code=postal_code,
        city=city,
        country_code=country_code,
    )


def parse_target(
    context: Context, name: str, address: Optional[Entity], date: str
) -> Optional[Entity]:
    name = " ".join(name.replace("\u00a0", " ").split())
    m = re.search(r"^(.+), (Frau|Herr[n]?) (.+)$", name)

    company_name = m.group(1).strip() if m else name.strip()
    company = context.make("Company")
    company.id = context.make_id("Company", company_name)
    company.add("name", company_name)
    h.apply_address(context, company, address)
    if m is None:
        return company

    gender = {"Frau": "female", "Herr": "male", "Herrn": "male"}[m.group(2)]
    w = m.group(3).split()
    if len(w) >= 3 and " ".join(w[-2:] + w[:-2]) in name:
        # "Silva Segovac Daniel, Herr Daniel Silva Segovac"
        given_name, family_name = " ".join(w[:-2]), " ".join(w[-2:])
    else:
        # "Schreindorfer Benedikt Clemens, Herr Benedikt Clemens Schreindorfer"
        # "Wilhelm Alexander, Herr Alexander Wilhelm"
        given_name, family_name = " ".join(w[:-1]), w[-1]
    person = context.make("Person")
    person.id = context.make_id("Person", given_name, family_name, gender)
    h.apply_name(person, given_name=given_name, last_name=family_name)
    person.add("gender", gender)
    company_name = company_name.removeprefix(f"{family_name} {given_name}")
    company_name = company_name.removeprefix(",").strip()
    if not company_name:
        h.apply_address(context, person, address)
        context.emit(person, target=True)
        return person

    emp = context.make("Employment")
    emp.id = context.make_id("Employment", person.id, company.id)
    emp.add("employee", person)
    emp.add("employer", company)
    emp.add("date", date)
    emp.add("role", "Manager found responsible for breaking the law")
    context.emit(person, target=False)
    context.emit(emp, target=False)

    return company


def extract_rows(pdf: IOBase) -> list[str]:
    # PDFMiner has a built-in layout analysis engine, but does not work well
    # for the layout of the debarment PDFs published by Liechtenstein.
    # Therefore we find all text boxes on a page, group them by lines,
    # and then extract (and clean up) the text in those boxes.
    lines = [[]]
    params = LAParams(boxes_flow=None, line_margin=0)
    for page in extract_pages(pdf, laparams=params):
        y = 1e9
        boxes = [b for b in page if isinstance(b, LTTextBoxHorizontal)]
        for box in sorted(boxes, key=lambda b: -b.y0):
            if abs(box.y0 - y) < 2.0:
                lines[-1].append(box)
            else:
                lines.append([box])
                y = box.y0
    # PDFMiner is sometimes too eager grouping words into text boxes.
    # For the Liechtenstein PDFs, we can work around this.
    fixup = re.compile(r"\d{2}\.\d{2}\.\d{4} .+$")
    rows = []
    for line in lines:
        row = [
            # Clean up whitespace.
            " ".join(box.get_text().replace("\u00A0", " ").split())
            for box in sorted(line, key=lambda b: b.x0)
        ]
        if len(row) > 0:
            if fixup.match(row[0]):
                date, rest = row[0].split(" ", 1)
                row = [date, rest] + row[1:]
            rows.append(row)
    return rows


def crawl_debarments(context: Context) -> None:
    path = context.fetch_resource("sperren.pdf", context.data_url)
    with open(path, "rb") as pdf:
        rows = extract_rows(pdf)
    if data_time := parse_data_time(rows):
        context.log.info(f"Parsing data version of {data_time}")
        context.data_time = data_time
    else:
        context.log.warn("Failed to parse data_time")
    for row in rows:
        if len(row) != 5 or row[-1] == "Ende der Sperre":
            continue
        [start, name, addr, law, end] = row
        address = parse_address(context, addr)
        start = h.parse_date(start, ["%d.%m.%Y"])
        end = h.parse_date(end, ["%d.%m.%Y"])
        company = parse_target(context, name, address, start)
        if company is None:
            continue
        company.add("topics", "debarment")
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id(
            "Sanction", "Debarment", company.id, law, start, end
        )
        sanction.add("startDate", start)
        sanction.add("endDate", end)
        sanction.add("description", "Debarment")
        sanction.add("program", "EntsG Sanctions")
        reason = (
            "Repeated or severe infraction against "
            f"Liechtenstein Posted Workers Act, {law}"
        )
        sanction.add("reason", reason)
        context.emit(sanction)
        context.emit(company, target=True)


def crawl_infractions(context: Context) -> None:
    path = context.fetch_resource("uebertretungen.pdf", context.data_url)
    with open(path, "rb") as pdf:
        rows = extract_rows(pdf)
    if data_time := parse_data_time(rows):
        context.log.info(f"Parsing data version of {data_time}")
        context.data_time = data_time
    else:
        context.log.warn("Failed to parse data_time")
    for row in rows:
        if len(row) == 1 or row[0].startswith("In Rechtskraft"):
            continue
        if len(row) != 4:
            context.log.warn(f"Cannot split row: {row}")
            continue
        [date, name, addr, law] = row
        address = parse_address(context, addr)
        date = h.parse_date(date, ["%d.%m.%Y"])
        company = parse_target(context, name, address, date)
        if company is None:
            continue
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id("Sanction", "Penalty", company.id, law, date)
        sanction.add("date", date)
        sanction.add("description", "Administrative Penalty")
        sanction.add("program", "EntsG Sanctions")
        sanction.add(
            "reason", f"Infraction against Liechtenstein Posted Workers Act, {law}"
        )
        company.add("topics", "debarment")
        context.emit(sanction)
        context.emit(company, target=True)
