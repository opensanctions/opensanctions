from pathlib import Path
from typing import IO, Dict, Generator, List, Tuple
from lxml import etree
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h

ODS = "application/vnd.oasis.opendocument.spreadsheet"
CellValue = Tuple[str, str, str]
PROGRAM_KEY = "NL-UNSC1373"


def read_table(
    fh: IO[bytes], sheet_name: str
) -> Generator[List[CellValue], None, None]:
    doc = etree.parse(fh)
    doc = h.remove_namespace(doc)
    for sheet in doc.findall(f"/body/spreadsheet/table[@name='{sheet_name}']"):
        for row in sheet.findall(".//table-row"):
            # print(inspect(row))
            cells: List[CellValue] = []
            for cell in row.findall("./table-cell"):
                value = text = str(cell.xpath("string()")) or ""
                type_ = cell.get("value-type") or "empty"
                if type_ == "date":
                    value = cell.get("date-value") or value
                else:
                    link = cell.find(".//a")
                    if link is not None:
                        type_ = "link"
                        value = link.get("href") or value
                repeated = int(cell.get("number-columns-repeated") or "1")
                repeated = min(255, repeated)
                for _ in range(repeated):
                    cells.append((type_, value, text))
            yield cells


def parse_sheet(path: Path) -> Generator[Dict[str, CellValue], None, None]:
    # ods is a bunch of xmls in a zip
    with ZipFile(path, "r") as zipfh:
        for name in zipfh.namelist():
            # content.xml contains the actual values.
            if name == "content.xml":
                with zipfh.open(name) as fh:
                    headers = None
                    for row in read_table(fh, "Sheet1"):
                        filled = [c[1] for c in row if len(c[1])]
                        if len(filled) < 4:
                            continue
                        if headers is None:
                            headers = [c[1] for c in row]
                            continue
                        yield dict(zip(headers, row))


def crawl(context: Context):
    # this file is ods (open document sheet)
    path = context.fetch_resource("source.ods", context.data_url)
    context.export_resource(path, ODS, title=context.SOURCE_TITLE)
    for row in parse_sheet(path):
        entity = context.make("LegalEntity")
        values = [v for _, (_, v, _) in row.items()]
        entity.id = context.make_id(*values)
        if entity.id is None:
            continue
        entity.add("topics", "crime")

        sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
        surname = row.pop("Surname")[1]
        first_name = row.pop("First name(s)")[1]
        entity.add("alias", row.pop("Alias")[1])
        sanction.add(
            "listingDate", row.pop("Date of ministerial decision (DD/MM/JJJJ)")[1]
        )
        sanction.add("sourceUrl", row.get("Link official notification")[1])
        sanction.add("authorityId", row.pop("Link official notification")[2])

        name = "%s %s" % (first_name, surname)
        name = name.strip()
        if not len(name):
            context.log.warning("No name", entity=entity, name=name)
        entity.add("name", name)
        entity.add_cast("Person", "firstName", first_name)

        birth_date = row.pop("Date of Birth(DD-MM-JJJJ)")
        if entity.schema.is_a("Person"):
            entity.add("lastName", surname)
            entity.add("birthDate", birth_date[1])
            entity.add("birthPlace", row.pop("Place of Birth")[1])
        else:
            entity.add("incorporationDate", birth_date[1])
            entity.add("notes", row.pop("Place of Birth")[1].replace("n/a: ", ""))

        context.emit(sanction)
        context.emit(entity)
