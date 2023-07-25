import csv
from pathlib import Path
from zipfile import ZipFile
from io import TextIOWrapper
from typing import Any, Dict, Generator
from pantomime.types import ZIP

from zavod import Context
from opensanctions import helpers as h

HEADERS = [
    "name_eng",
    "name_rus",
    "name_ukr",
    "date_of_birth",
    "nationality",
    "source_url",
]


def read_zip_csv(context: Context, path: Path) -> Generator[Dict[str, Any], None, None]:
    with ZipFile(path, "r") as zipfh:
        for name in zipfh.namelist():
            context.log.info("Reading: %s in %s" % (name, path))
            with zipfh.open(name, "r") as fhb:
                fh = TextIOWrapper(fhb, encoding="utf-8")
                reader = csv.reader(fh, delimiter=",")
                for row in reader:
                    yield dict(zip(HEADERS, row))


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.zip", context.data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)

    for data in read_zip_csv(context, path):
        source_url = data.get("source_url")
        entity = context.make("Person")
        entity.id = context.make_id(source_url)
        entity.add("name", data.get("name_eng"), lang="eng")
        entity.add("name", data.get("name_rus"), lang="rus")
        entity.add("name", data.get("name_ukr"), lang="ukr")
        for country in h.multi_split(data.get("nationality"), [",", ";", "/"]):
            entity.add("country", country, lang="rus")
        entity.add("birthDate", data.get("date_of_birth"))
        entity.add("sourceUrl", source_url)
        entity.add("topics", "crime.war")
        context.emit(entity, target=True)
