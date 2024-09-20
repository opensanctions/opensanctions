from followthemoney.types import registry
from typing import Dict
import re

from zavod import Context, Entity
from zavod import helpers as h

TYPES = {"osoby": "Person", "podmioty": "Company"}
BDAY_FORMATS = ("%d.%m.%Y", "%d %b %Y", "%d %B %Y")
CHOPSKA = [
    ("Nr NIP", "taxNumber"),
    ("NIP", "taxNumber"),
    ("Nr KRS", "registrationNumber"),
    ("KRS", "registrationNumber"),
    ("(PESEL:", "idNumber"),
    ("PESEL:", "idNumber"),
    ("siedziba:", "address"),
    ("adres:", "address"),
    (" r. w ", "birthPlace"),  # "year in" in Polish
]


def parse_date(text, context):
    text = text.lower().strip()
    text = text.replace("urodzona", "")
    text = text.replace("urodzonego", "")
    text = text.replace("urodzony", "")
    text = text.replace("urodzonej", "")
    text = re.split(r" r\.| r$", text)[0]
    text = text.strip()
    text = h.replace_months(context.dataset, text)
    text = context.lookup_value("preparse_date", text, text)
    if text is None:
        return None
    date_info = h.parse_formats(text, context.dataset.dates.formats)
    if date_info and date_info.dt:
        return date_info.text  # Return the parsed date as a string
    context.log.warning("Failed to parse date", raw_date=text)
    return None


def parse_details(context: Context, entity: Entity, text: str):
    for chop, prop in CHOPSKA:
        parts = text.rsplit(chop, 1)
        text = parts[0]
        if len(parts) > 1:
            entity.add(prop, parts[1].strip())

    if not len(text.strip()):
        return

    bday = parse_date(text, context)
    if bday:
        h.apply_date(entity, "birthDate", bday)
        text = re.sub(r"urodzon. \d+[. ]\w+[. ]\d+ r\.?", "", text).strip()

    if text == "":
        return
    result = context.lookup("details", text)
    if result is None:
        context.log.warning("Unhandled details", details=repr(text))
    else:
        for prop, value in result.props.items():
            entity.add(prop, value)


def crawl_row(context: Context, row: Dict[str, str], table_title: str):
    listing_date = row.pop("data_umieszczenia_na_liscie")
    if listing_date is None:
        context.log.warn("No listing date", row=row)
        return

    entity = context.make(TYPES[table_title])
    name = row.pop("nazwisko_i_imie", None)
    name = row.pop("nazwa_podmiotu", name)
    if name is None:
        context.log.warn("No name", row=row)
        return

    entity.id = context.make_slug(table_title, name)
    names = name.split("(")
    entity.add("name", names[0])
    for alias in names[1:]:
        entity.add("alias", alias.split(")")[0])
    notes = row.pop("uzasadnienie_wpisu_na_liste")
    entity.add("notes", notes)

    details = row.pop("dane_identyfikacyjne_podmiotu", None)
    details = row.pop("dane_identyfikacyjne_osoby", details)
    if details is not None:
        parse_details(context, entity, details)

    sanction = h.make_sanction(context, entity)
    provisions = row.pop("zastosowane_srodki_sankcyjne")
    if len(provisions) > registry.string.max_length:
        sanction.add("description", provisions)
        sanction.add("provisions", "See description.")
    else:
        sanction.add("provisions", provisions)

    h.apply_date(sanction, "startDate", listing_date)
    end_date = row.pop("data_wykreslenia_z_listy", None)
    h.apply_date(sanction, "endDate", end_date)
    if not end_date:
        entity.add("topics", "sanction")
    context.audit_data(row)
    context.emit(entity, target=not end_date)
    context.emit(sanction)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    table = doc.xpath(".//h3[text() = 'Osoby']/following-sibling::div//table")[0]
    for row in h.parse_html_table(table, header_tag="td"):
        crawl_row(context, h.cells_to_str(row), "osoby")

    # Pretty special xpath because they have some <table><tr><table> thing going on
    table = doc.xpath(
        ".//h3[text() = 'Podmioty']/following-sibling::div//table//tr//table"
    )[0]
    for row in h.parse_html_table(table, header_tag="td"):
        crawl_row(context, h.cells_to_str(row), "podmioty")
