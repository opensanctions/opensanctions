from typing import Dict
from followthemoney.types import registry

from zavod import Context, Entity
from zavod import helpers as h

TYPES = {"osoby": "Person", "podmioty": "Company"}
BDAY_FORMATS = ("%d.%m.%Y", "%d %b %Y")
MONTHS = {
    "stycznia": "jan",
    "lutego": "feb",
    "marca": "mar",
    "kwietnia": "apr",
    "maja": "may",
    "czerwca": "jun",
    "lipca": "jul",
    "lipiec": "jul",
    "sierpnia": "aug",
    "września": "sep",
    "października": "oct",
    "listopada": "nov",
    "grudnia": "dec",
}
CHOPSKA = [
    ("Nr NIP", "taxNumber"),
    ("NIP", "taxNumber"),
    ("Nr KRS", "registrationNumber"),
    ("KRS", "registrationNumber"),
    ("siedziba:", "address"),
]


def parse_date(text):
    text = text.lower().strip()
    text = text.replace("urodzona", "")
    text = text.replace("urodzonego", "")
    text = text.replace("urodzony", "")
    text = text.rstrip(" r.")
    text = text.rstrip(" r.,")
    text = text.rstrip("r")
    text = text.strip()
    for pl, en in MONTHS.items():
        text = text.replace(pl, en)
    prefix = h.parse_formats(text, BDAY_FORMATS)
    if prefix.dt:
        return prefix.text


def parse_details(context: Context, entity: Entity, text: str):
    for chop, prop in CHOPSKA:
        parts = text.rsplit(chop, 1)
        text = parts[0]
        if len(parts) > 1:
            entity.add(prop, parts[1])

    if not len(text.strip()):
        return
    bday = parse_date(text)
    if bday:
        entity.add("birthDate", bday)
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
