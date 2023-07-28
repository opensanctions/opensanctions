from pathlib import Path
from typing import IO, Generator, List, Tuple
from lxml import etree
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h

ODS = "application/vnd.oasis.opendocument.spreadsheet"
CellValue = Tuple[str, str, str]


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


def parse_sheet(path: Path) -> Generator[Tuple[Tuple[str, CellValue], ...], None, None]:
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
                        cells = list(zip(headers, row))
                        yield tuple(cells)


def crawl(context: Context):
    # this file is ods (open document sheet)
    path = context.fetch_resource("source.ods", context.data_url)
    context.export_resource(path, ODS, title=context.SOURCE_TITLE)
    for row in parse_sheet(path):
        entity = context.make("LegalEntity")
        entity.add("topics", "crime")
        values = [v for _, (_, v, _) in row]
        entity.id = context.make_id(*values)
        if entity.id is None:
            continue

        sanction = h.make_sanction(context, entity)
        surname, first_name = None, None

        for field, (_, value, text) in row:
            if "Surname" in field:
                surname = value
            if "First name" in field:
                first_name = value
            if "Alias" in field:
                entity.add("alias", value)
            if "Date of Birth" in field:
                if "n/a" not in value:
                    entity.add_cast("Person", "birthDate", value)
            if "Place of Birth" in field:
                entity.add_cast("Person", "birthPlace", value)
            if "Date of ministerial decisio" in field:
                sanction.add("listingDate", value)
            if "Link official notification" in field:
                sanction.add("sourceUrl", value)
                sanction.add("authorityId", text)

        name = "%s %s" % (first_name, surname)
        name = name.strip()
        if not len(name):
            context.log.warning("No name", entity=entity, name=name)
        entity.add("name", name)
        entity.add("firstName", first_name, quiet=True)
        entity.add("lastName", surname, quiet=True)

        context.emit(sanction)
        context.emit(entity, target=True)
