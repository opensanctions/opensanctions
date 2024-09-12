import re
import lxml
import shutil
from datetime import datetime
from typing import Optional

from zavod import Context, Entity
from zavod import helpers as h


COUNTRY_CODES = {
    "A": "at",  # Austria
    "D": "de",  # Germany
    "F": "fr",  # France
    "I": "it",  # Italy
    "SL": "si",  # Slovenia
}


def parse_data_time(doc, context) -> Optional[datetime]:
    text = doc.xpath("//p[starts-with(., 'Stand:')]/strong")[0].text
    text = h.replace_months(context.dataset, text)
    if date := h.parse_date(text, context.dataset.dates.formats):
        return datetime.strptime(date[0], "%Y-%m-%d")
    else:
        return None


def parse_address(context: Context, addr: str) -> Optional[Entity]:
    addr_clean = context.lookup_value("address_override", addr, default=addr)
    if addr_clean is None:
        return None
    parts = [p.strip() for p in addr_clean.split(",")]
    street = parts[0]
    country_code, postal_code, city = None, None, None
    if m := re.match(r"(A|D|F|I|[A-Z]{2})[- ]\s*([\d\-]+) (.+)", parts[-1]):
        country_code = COUNTRY_CODES.get(m.group(1), m.group(1).lower())
        postal_code, city = m.group(2), m.group(3)
    if not country_code or not city:
        context.log.warn(f'Cannot parse address "{addr_clean}"')
    return h.make_address(
        context=context,
        full=addr_clean,
        street=street,
        postal_code=postal_code,
        city=city,
        country_code=country_code,
    )


def parse_target(
    context: Context, name: str, address: Optional[Entity], date: str
) -> Optional[Entity]:
    name = " ".join(name.replace("\u00a0", " ").split())
    person = context.make("Person")
    m = re.search(r"^(.+), (Frau|Herr[n]?) (.+)$", name)
    if m is None:
        context.log.warn(f'Cannot parse target "{name}"')
        return None
    company_name = m.group(1)
    gender = {"Frau": "female", "Herr": "male", "Herrn": "male"}[m.group(2)]
    w = m.group(3).split()
    if len(w) >= 3 and " ".join(w[-2:] + w[:-2]) in name:
        # "Silva Segovac Daniel, Herr Daniel Silva Segovac"
        given_name, family_name = " ".join(w[:-2]), " ".join(w[-2:])
    else:
        # "Schreindorfer Benedikt Clemens, Herr Benedikt Clemens Schreindorfer"
        # "Wilhelm Alexander, Herr Alexander Wilhelm"
        given_name, family_name = " ".join(w[:-1]), w[-1]
    person.id = context.make_id("Person", given_name, family_name, gender)
    h.apply_name(person, given_name=given_name, last_name=family_name)
    person.add("gender", gender)
    company_name = company_name.removeprefix(f"{family_name} {given_name}")
    company_name = company_name.removeprefix(",").strip()
    if not company_name:
        h.apply_address(context, person, address)
        context.emit(person, target=True)
        return person

    company = context.make("Company")
    company.id = context.make_id("Company", company_name)
    company.add("name", company_name)
    h.apply_address(context, company, address)

    emp = context.make("Employment")
    emp.id = context.make_id("Employment", person.id, company.id)
    emp.add("employee", person)
    emp.add("employer", company)
    h.apply_date(emp, "date", date)
    emp.add("role", "Manager found responsible for breaking the law")
    context.emit(person, target=False)
    context.emit(emp, target=False)

    return company


def parse_debarments(context: Context, doc) -> None:
    table = doc.xpath(
        "//h2[text()='Laufende und abgelaufene Entsendesperren"
        + " (Art. 7 Abs. 2 Entsendegesetz)']/following::table[1]"
    )[0]
    for row in table.xpath("tbody/tr")[1:]:
        [start, name, addr, law, end] = row.xpath("descendant::*/text()")
        address = parse_address(context, addr)
        company = parse_target(context, name, address, start)
        if company is None:
            continue
        company.add("topics", "debarment")
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id(
            "Sanction", "Debarment", company.id, law, start, end
        )
        h.apply_date(sanction, "date", start)
        h.apply_date(sanction, "endDate", end)
        sanction.add("description", "Debarment")
        sanction.add("program", "EntsG Sanctions")
        reason = (
            "Repeated or severe infraction against "
            f"Liechtenstein Posted Workers Act, {law}"
        )
        sanction.add("reason", reason)
        context.emit(sanction)
        context.emit(company, target=True)


def parse_infractions(context: Context, doc) -> None:
    table = doc.xpath(
        "//h2[text()='Übertretungen (Art. 9 Entsendegesetz)']/following::table[1]"
    )[0]
    for row in table.xpath("tbody/tr")[1:]:
        [date, name, addr, law] = row.xpath("descendant::*/text()")
        address = parse_address(context, addr)
        company = parse_target(context, name, address, date)
        if company is None:
            continue
        sanction = h.make_sanction(context, company)
        sanction.id = context.make_id("Sanction", "Penalty", company.id, law, date)
        h.apply_date(sanction, "date", date)
        sanction.add("description", "Administrative Penalty")
        sanction.add("program", "EntsG Sanctions")
        sanction.add(
            "reason", f"Infraction against Liechtenstein Posted Workers Act, {law}"
        )
        company.add("topics", "debarment")
        context.emit(sanction)
        context.emit(company, target=True)


def crawl(context: Context):
    assert context.dataset.base_path is not None
    data_path = context.dataset.base_path / "data.html"
    source_path = context.get_resource_path("source.html")
    shutil.copyfile(data_path, source_path)
    # source_path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(source_path, "text/html", title="Source HTML file")
    with open(source_path, "r") as fh:
        doc = lxml.html.fromstring(fh.read())  # invalid XML, need HTML parser
    if data_time := parse_data_time(doc, context):
        context.log.info(f"Parsing data version of {data_time}")
        context.data_time = data_time
    else:
        context.log.warn("Failed to parse data_time")
    parse_debarments(context, doc)
    parse_infractions(context, doc)
