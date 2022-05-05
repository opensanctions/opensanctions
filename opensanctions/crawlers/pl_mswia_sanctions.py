from lxml import html
from normality import collapse_spaces, slugify
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h

TYPES = {"OSOBY": "Person", "PODMIOTY": "Company"}
CHOPSKA = [
    ("Nr NIP", "taxNumber"),
    ("NIP", "taxNumber"),
    ("Nr KRS", "registrationNumber"),
    ("KRS", "registrationNumber"),
    ("siedziba:", "address"),
]


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.dataset.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8") as fh:
        doc = html.fromstring(fh.read())
    for table in doc.findall('.//div[@class="editor-content"]//table'):
        headers = None
        schema = None
        for row in table.findall(".//tr"):
            cells = [collapse_spaces(c.text_content()) for c in row.findall("./td")]
            if headers is None:
                headers = [slugify(c, sep="_") for c in cells]
                continue
            if len(cells) == 1:
                schema = TYPES[cells[0]]
                continue
            row = dict(zip(headers, cells))

            entity = context.make(schema)
            name = row.pop("imie_i_nazwisko_nazwa_podmiotu")
            entity.id = context.make_slug(name)
            names = name.split("(")
            entity.add("name", names[0])
            for alias in names[1:]:
                entity.add("alias", alias.split(")")[0])
            notes = row.pop("uzasadnienie_wpisu_na_liste")
            entity.add("notes", notes)

            details = row.pop("dane_identyfikacyjne_osoby_podmiotu")
            for (chop, prop) in CHOPSKA:
                parts = details.rsplit(chop, 1)
                details = parts[0]
                if len(parts) > 1:
                    if prop == "address":
                        addr = h.make_address(context, full=parts[1])
                        h.apply_address(context, entity, addr)
                    else:
                        entity.add(prop, parts[1])
            if len(details.strip()):
                result = context.lookup("details", details)
                if result is None:
                    context.log.warning("Unhandled details", details=details)
                else:
                    for prop, value in result.props.items():
                        entity.add(prop, value)

            sanction = h.make_sanction(context, entity)
            provisions = row.pop("zastosowane_srodki_sankcyjne")
            sanction.add("provisions", provisions)

            start_date = row.pop("data_umieszczenia_na_liscie")
            sanction.add("startDate", h.parse_date(start_date, ["%d.%m.%Y"]))

            h.audit_data(row)
            context.emit(entity, target=True)
            context.emit(sanction)
