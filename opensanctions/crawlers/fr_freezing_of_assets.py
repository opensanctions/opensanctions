import zipfile
from pprint import pprint  # noqa
from lxml import html
import xlrd
from xlrd.xldate import xldate_as_datetime
from urllib.parse import urljoin


NATURES = {"personne physique": "Person", "personne morale": "Company"}


def parse_reference(context, row: dict):
    schema = NATURES[row.pop("nature")]
    entity = context.make(schema)

    first_name = row.pop("firstname")
    last_name = row.pop("lastname")
    birth_date = row.pop("birthdate")
    entity.make_id("FREEZING", "{}{}{}".format(first_name, last_name, birth_date))

    entity.add("birthDate", birth_date, quiet=True)
    entity.add("birthPlace", row.pop("birthplace"))
    entity.add("name", "{} {}".format(first_name, last_name))
    entity.add("firstName", first_name)
    entity.add("lastName", last_name)
    entity.add("alias", row.pop("aliases"))
    # entity.add("keywords", "ASSETS_FREEZING")

    context.emit(entity)


def crawl(context):
    res = context.http.get(context.dataset.data.url)
    doc = html.fromstring(res.text)
    url_file = doc.find(".//a[@class='download']")
    url_file = urljoin(res.url, url_file.get("href"))

    context.fetch_artifact("source.zip", url_file)
    path = context.get_artifact_path("source.zip")
    zfp = zipfile.ZipFile(path, "r")
    info = zfp.filelist[0]
    fp = zfp.read(info)

    xls = xlrd.open_workbook(file_contents=fp)
    ws = xls.sheet_by_index(1)
    headers = [
        "firstname",
        "lastname",
        "aliases",
        "birthdate",
        "birthplace",
        "other informations",
        "nature",
    ]

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
                row[header] = None
            else:
                row[header] = cell.value

        parse_reference(context, row)
