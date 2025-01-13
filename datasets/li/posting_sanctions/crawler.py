import re
from typing import Dict, Optional, Tuple
from pdfplumber.page import Page

from zavod import Context, Entity
from zavod import helpers as h

DEBARMENT_URL = "https://www.llv.li/serviceportal2/amtsstellen/amt-fuer-volkswirtschaft/wirtschaft/entsendegesetz/sperren.pdf"
INFRACTION_URL = "https://www.llv.li/serviceportal2/amtsstellen/amt-fuer-volkswirtschaft/wirtschaft/entsendegesetz/uebertretungen.pdf"


COUNTRY_CODES = {
    "A": "at",  # Austria
    "D": "de",  # Germany
    "F": "fr",  # France
    "I": "it",  # Italy
    "CH": "ch",  # Switzerland
    "SL": "si",  # Slovenia
}
TITLE_GENDER = {
    "Frau": "female",
    "Herr": "male",
    "Herrn": "male",
}


def parse_address(context: Context, addr: str) -> Optional[Entity]:
    addr = addr.replace("‐", "-")
    addr = context.lookup_value("address_override", addr, default=addr)
    parts = [p.strip() for p in addr.split(",")]
    street = parts[0]
    country_code, postal_code, city = None, None, None
    if m := re.match(r"(A|D|F|I|[A-Z]{2})-\s*([\d\-]+) (.+)", parts[-1], re.UNICODE):
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


def crawl_named(
    context: Context,
    name: str,
    address: Optional[Entity],
    date: str,
    url: str,
    type: str,
) -> Optional[Entity]:
    """
    Parses the subjects named in the list. If a company name is split from a
    persons, we emit the person and their relationship, and return the company.
    Otherwise the person is returned.
    """
    name = " ".join(name.replace("\u00a0", " ").split())
    m = re.search(r"^(.+), (Frau|Herr[n]?) (.+)$", name)

    company_name = m.group(1).strip() if m else name.strip()
    company = context.make("Company")
    company.id = context.make_id("Company", company_name)
    company.add("name", company_name)
    h.copy_address(company, address)
    if m is None:
        return company

    gender = TITLE_GENDER[m.group(2)]
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
        return person

    emp = context.make("Employment")
    emp.id = context.make_id("Employment", person.id, company.id, date, type)
    emp.add("employee", person)
    emp.add("employer", company)
    h.apply_date(emp, "date", date)
    emp.add("role", "Manager found responsible for breaking the law")
    emp.add("sourceUrl", url)
    context.emit(person, target=False)
    context.emit(emp, target=False)
    return company


def page_settings(page: Page) -> Tuple[Page, Dict]:
    cropped = page.crop((0, 50, page.width, page.height - 10))
    settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "text_tolerance": 1,
    }
    return cropped, settings


def crawl_debarments(context: Context) -> None:
    path = context.fetch_resource("sperren.pdf", DEBARMENT_URL)
    for row in h.parse_pdf_table(context, path, page_settings=page_settings):
        if len(row) != 5:
            continue
        address = parse_address(context, row.pop("adresse"))
        name = row.pop("betrieb")
        effective = row.pop("in_rechtskraft")
        end = row.pop("ende_der_sperre")
        entity = crawl_named(
            context, name, address, effective, DEBARMENT_URL, "Debarment"
        )
        if entity is None:
            continue
        violation = row.pop("verstoss")
        sanction = h.make_sanction(context, entity)
        sanction.id = context.make_id(
            "Sanction", "Debarment", entity.id, violation, effective, end
        )
        h.apply_date(sanction, "startDate", effective)
        h.apply_date(sanction, "endDate", end)
        sanction.add("description", "Debarment")
        sanction.add("program", "EntsG Sanctions")
        sanction.add("reason", violation)
        sanction.add("sourceUrl", DEBARMENT_URL)

        is_debarred = h.is_active(sanction)
        if is_debarred:
            entity.add("topics", "debarment")

        context.emit(sanction)
        context.emit(entity, target=is_debarred)


def crawl_infractions(context: Context) -> None:
    path = context.fetch_resource("uebertretungen.pdf", INFRACTION_URL)
    for row in h.parse_pdf_table(context, path, page_settings=page_settings):
        if len(row) != 4:
            context.log.warn(f"Cannot split row: {row}")
            continue
        address = parse_address(context, row.pop("adresse"))
        effective = row.pop("in_rechtskraft")
        name = row.pop("betrieb_verantwortliche_naturliche_person")
        entity = crawl_named(
            context, name, address, effective, INFRACTION_URL, "Infraction"
        )
        if entity is None:
            continue
        entity.add("topics", "reg.warn")
        violation = row.pop("verstoss")
        sanction = h.make_sanction(context, entity)
        sanction.id = context.make_id(
            "Sanction", "Penalty", entity.id, violation, effective
        )
        h.apply_date(sanction, "date", effective)
        sanction.add("description", "Administrative Penalty")
        sanction.add("program", "EntsG Sanctions")
        sanction.add("reason", violation)
        sanction.add("sourceUrl", INFRACTION_URL)
        context.emit(sanction)
        context.emit(entity, target=True)


def crawl(context: Context) -> None:
    crawl_debarments(context)
    crawl_infractions(context)
