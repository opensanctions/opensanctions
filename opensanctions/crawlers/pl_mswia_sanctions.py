import openpyxl
from datetime import datetime
from urllib.parse import urljoin
from collections import defaultdict
from typing import List, Optional
from normality import slugify
from pantomime.types import HTML, XLSX

from opensanctions.core import Context, Entity
from opensanctions import helpers as h

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
    for (chop, prop) in CHOPSKA:
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


def crawl_excel(context: Context, url: str):
    path = context.fetch_resource("source.xlsx", url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    for sheet in workbook.worksheets:
        schema = TYPES[sheet.title]
        headers: Optional[List[str]] = None
        for row in sheet.rows:
            cells = [c.value for c in row]
            if headers is None:
                headers = [slugify(h, sep="_") for h in cells]
                continue
            row = dict(zip(headers, cells))
            listing_date = row.pop("data_umieszczenia_na_liscie")
            if listing_date is None:
                # context.log.warn("No listing date", row=row)
                continue

            entity = context.make(schema)
            name = row.pop("nazwisko_i_imie", None)
            name = row.pop("nazwa_podmiotu", name)
            if name is None:
                context.log.warn("No name", row=row)
                return

            entity.id = context.make_slug(sheet.title, name)
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
            sanction.add("provisions", provisions)

            sanction.add("startDate", listing_date)
            sanction.add("endDate", row.pop("data_wykreslenia_z_listy", None))

            context.audit_data(row)
            context.emit(entity, target=True)
            context.emit(sanction)


def crawl(context: Context):
    doc = context.fetch_html(context.source.data.url)

    xlsx_found = False
    for a in doc.findall(".//a[@class='file-download']"):
        if ".xlsx" in a.text_content():
            xlsx_found = True
            url = urljoin(context.source.data.url, a.get("href"))
            crawl_excel(context, url)

    if not xlsx_found:
        context.log.error("Could not find XLSX file")
