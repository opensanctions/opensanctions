import re
import pdfplumber
from pathlib import Path
from typing import Dict, List, Optional

from zavod import Context, Entity
from zavod import helpers as h

DEBARMENT_URL = "https://www.llv.li/serviceportal2/amtsstellen/amt-fuer-volkswirtschaft/wirtschaft/entsendegesetz/sperren.pdf"
INFRACTION_URL = "https://www.llv.li/serviceportal2/amtsstellen/amt-fuer-volkswirtschaft/wirtschaft/entsendegesetz/uebertretungen.pdf"


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
    h.copy_address(company, address)
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


def extract_rows(path: Path) -> List[Dict[str, str]]:
    pdf = pdfplumber.open(path.as_posix())
    settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "text_tolerance": 1,
    }
    headers: Optional[List[str]] = None
    rows: List[Dict[str, str]] = []
    for page in pdf.pages:
        cropped = page.crop((0, 50, page.width, page.height - 10))
        for row in cropped.extract_table(settings):
            if headers is None:
                headers = row
                continue
            rows.append({k: v for k, v in zip(headers, row)})
    return rows


def crawl_debarments(context: Context) -> None:
    path = context.fetch_resource("sperren.pdf", DEBARMENT_URL)
    for row in extract_rows(path):
        if len(row) != 5:
            continue
        address = parse_address(context, row.pop("Adresse"))
        name = row.pop("Betrieb")
        start = h.parse_date(row.pop("In Rechtskraft"), ["%d.%m.%Y"])
        end = h.parse_date(row.pop("Ende der Sperre"), ["%d.%m.%Y"])
        company = parse_target(context, name, address, start)
        if company is None:
            continue
        company.add("topics", "debarment")
        violation = row.pop("Verstoss")
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id(
            "Sanction", "Debarment", company.id, violation, start, end
        )
        sanction.add("startDate", start)
        sanction.add("endDate", end)
        sanction.add("description", "Debarment")
        sanction.add("program", "EntsG Sanctions")
        reason = (
            "Repeated or severe infraction against "
            f"Liechtenstein Posted Workers Act, {violation}"
        )
        sanction.add("reason", reason)
        context.emit(sanction)
        context.emit(company, target=True)


def crawl_infractions(context: Context) -> None:
    path = context.fetch_resource("uebertretungen.pdf", INFRACTION_URL)
    for row in extract_rows(path):
        if len(row) != 4:
            context.log.warn(f"Cannot split row: {row}")
            continue
        address = parse_address(context, row.pop("Adresse"))
        effective = row.pop("In Rechtskraft")
        name = row.pop("Betrieb/ verantwortliche natürliche Person")
        date = h.parse_date(effective, ["%d.%m.%Y"])
        company = parse_target(context, name, address, date)
        if company is None:
            continue
        violation = row.pop("Verstoss")
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id(
            "Sanction", "Penalty", company.id, violation, date
        )
        sanction.add("date", date)
        sanction.add("description", "Administrative Penalty")
        sanction.add("program", "EntsG Sanctions")
        sanction.add(
            "reason",
            f"Infraction against Liechtenstein Posted Workers Act, {violation}",
        )
        company.add("topics", "debarment")
        context.emit(sanction)
        context.emit(company, target=True)


def crawl(context: Context) -> None:
    crawl_debarments(context)
    crawl_infractions(context)
