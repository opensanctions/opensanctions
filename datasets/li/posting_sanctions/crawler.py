from datetime import datetime
import lxml
import re

from zavod import Context, Entity
from zavod import helpers as h


MONTHS_DE = {
    # We won’t know until January 2024 if the site uses the Austrian name
    # or the German/Swiss name for the month January.
    "Januar": "January",
    "Jänner": "January",
    "Februar": "February",
    "März": "March",
    "Mai": "May",
    "Juni": "June",
    "Juli": "July",
    "Oktober": "October",
    "Dezember": "December",
}


COUNTRY_CODES = {
    "A": "at",   # Austria
    "D": "de",   # Germany
    "F": "fr",   # France
    "I": "it",   # Italy
    "SL": "si",  # Slovenia
}


ADDRESS_FIXES = {
    "Alte Tiefenaustrasse 6, 3050 Bern": "Alte Tiefenaustrasse 6, CH-3050 Bern",
    "Bachwisenstrasse 7c, 9200 CH-Gossau SG": "Bachwisenstrasse 7c, CH-9200 Gossau SG",
    "Industrie Allmend 31, 4629 Fulenbach": "Industrie Allmend 31, CH-4629 Fulenbach",
    "Hagenholzstrasse 81a, 8050 CH-Zürich": "Hagenholzstrasse 81a, CH-8050 Zürich",
}


def parse_data_time(doc) -> datetime:
    text = doc.xpath("//p[starts-with(., 'Stand:')]/strong")[0].text
    for de, en in MONTHS_DE.items():
        # Adding space to prevent replacing Jänner -> January -> Januaryy
        text = text.replace(de + " ", en + " ")
    if date := h.parse_date(text, ["%d. %B %Y"]):
        return datetime.strptime(date[0], "%Y-%m-%d")
    else:
        return None


def parse_address(context: Context, addr: str) -> Entity:
    addr = ADDRESS_FIXES.get(addr, addr)
    parts = [p.strip() for p in addr.split(",")]
    street = parts[0]
    country_code, postal_code, city = None, None, None
    if m := re.match(r"(A|D|F|I|[A-Z]{2})[- ]\s*([\d\-]+) (.+)", parts[-1]):
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


def parse_target(context: Context, name: str, address: Entity, date: str) -> Entity:
    name = " ".join(name.replace("\u00a0", " ").split())
    person = context.make("Person")
    m = re.search(r"^(.+), (Frau|Herr[n]?) (.+)$", name)
    if m == None:
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
    person.add("firstName", given_name)
    person.add("lastName", family_name)
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
    emp.add("date", date)
    emp.add("role", "Manager found responsible for breaking the law")

    context.emit(company, target=True)
    context.emit(person, target=False)
    context.emit(emp, target=False)

    return company


def parse_debarments(context, doc):
    table = doc.xpath(
        "//h2[text()='Laufende und abgelaufene Entsendesperren"
        + " (Art. 7 Abs. 2 Entsendegesetz)']/following::table[1]"
    )[0]
    for row in table.xpath("tbody/tr")[1:]:
        [start, name, addr, law, end] = row.xpath("descendant::*/text()")
        address = parse_address(context, addr)
        start = h.parse_date(start, ["%d.%m.%Y"])
        end = h.parse_date(end, ["%d.%m.%Y"])
        target = parse_target(context, name, address, start)
        target.add("topics", "debarment")
        sanction = h.make_sanction(context, target)
        sanction.id = context.make_id(
            "Sanction", "Debarment", target.id, law, start, end
        )
        sanction.add("startDate", start)
        sanction.add("endDate", end)
        sanction.add("description", "Debarment")
        sanction.add(
            "reason",
            f"Repeated or severe infraction against Liechtenstein Posted Workers Act, {law}",
        )
        context.emit(sanction)


def parse_infractions(context, doc):
    table = doc.xpath(
        "//h2[text()='Übertretungen (Art. 9 Entsendegesetz)']/following::table[1]"
    )[0]
    for row in table.xpath("tbody/tr")[1:]:
        [date, name, addr, law] = row.xpath("descendant::*/text()")
        address = parse_address(context, addr)
        date = h.parse_date(date, ["%d.%m.%Y"])
        target = parse_target(context, name, address, date)
        sanction = h.make_sanction(context, target)
        sanction.id = context.make_id("Sanction", "Penalty", target.id, law, date)
        sanction.add("date", date)
        sanction.add("description", "Administrative Penalty")
        sanction.add(
            "reason", f"Infraction against Liechtenstein Posted Workers Act, {law}"
        )
        context.emit(sanction)


def crawl(context: Context):
    source_path = context.fetch_resource("source.html", context.dataset.data.url)
    context.export_resource(source_path, "text/html", title="Source HTML file")
    with open(source_path, "r") as fh:
        doc = lxml.html.fromstring(fh.read())
    if data_time := parse_data_time(doc):
        context.log.info(f"Parsing data version of {data_time}")
        context.data_time = data_time
    else:
        context.log.warn("Failed to parse data_time")
    parse_debarments(context, doc)
    parse_infractions(context, doc)
