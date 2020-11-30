from pprint import pprint  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter
import requests
import io
import zipfile
import xlrd
from xlrd.xldate import xldate_as_datetime

NATURES = {"personne physique": "Physical person",
           "personne morale": "Corporation"}


def parse_reference(_emitter: EntityEmitter, _row: dict):
    nature = NATURES.get(_row.pop("nature"))
    first_name = _row.pop("firstname")
    last_name = _row.pop("lastname")
    birth_date = _row.pop("birthdate")

    if nature == NATURES.get("personne morale"):
        entity = _emitter.make("LegalEntity")
        entity.add("incorporationDate", birth_date)
    else:
        entity = _emitter.make("Person")
        entity.add("birthDate", birth_date)
        entity.add("birthPlace", _row.pop("birthplace"))

    entity.make_id("FREEZING", "{}{}{}".format(
        first_name, last_name, birth_date)
    )

    entity.add("name", "{} {}".format(
        first_name, last_name)
    )
    entity.add("alias", _row.pop("aliases"))

    entity.add("status", nature)
    entity.add("keywords", "ASSETS_FREEZING")

    _emitter.emit(entity)


def parse(context, data):
    emitter = EntityEmitter(context)
    items = []
    with context.http.rehash(data) as res:
        doc = res.html
        url_file = doc.find(".//main//ul//li//a[@href]")
        url_file = url_file.get('href')

        data = requests.get(url_file).content
        fp = io.BytesIO(data)
        zfp = zipfile.ZipFile(fp, "r")
        info = zfp.filelist[0]
        fp = zfp.read(info)

        xls = xlrd.open_workbook(file_contents=fp)
        ws = xls.sheet_by_index(1)
        headers = ['firstname', 'lastname', 'aliases',
                   'birthdate', 'birthplace', 'other informations', 'nature']

        for r in range(3, ws.nrows):
            row = ws.row(r)
            row = dict(zip(headers, row))
            for header, cell in row.items():
                if cell.ctype == 2:
                    row[header] = str(int(cell.value))
                elif cell.ctype == 3:
                    date = xldate_as_datetime(cell.value, xls.datemode)
                    row[header] = date.isoformat()
                elif cell.ctype == 0:
                    row[header] = ""
                else:
                    row[header] = cell.value

            items.append(row)

        for row in items:
            parse_reference(emitter, row)

        emitter.finalize()
