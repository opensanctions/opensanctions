import re
import csv
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html, fetch_resource

PROGRAM_KEY = "BR-CEIS"


def get_csv_url(context: Context) -> str:
    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator="//script[contains(text(), '\"dia\"')]",
        html_source="browserHtml",
        geolocation="BR",
    )
    date_pattern = re.compile(
        r'"ano"\s*:\s*"(\d+)",\s*"mes"\s*:\s*"(\d+)",\s*"dia"\s*:\s*"(\d+)"'
    )
    for script in doc.xpath("//script"):
        if script.text:
            match = date_pattern.search(script.text)
            if match:
                year, month, day = match.groups()
                return context.data_url + f"/{year}{month}{day}"
    raise ValueError("Data URL not found")


def crawl(context: Context) -> None:
    csv_url = get_csv_url(context)
    _, _, _, path = fetch_resource(context, "source.zip", csv_url, geolocation="BR")
    with ZipFile(path) as zip_file:
        file_name = zip_file.namelist()[0]
        csv_bytes = zip_file.read(file_name)

    lines = csv_bytes.decode("iso-8859-1").splitlines()
    reader = csv.DictReader(lines, delimiter=";")

    for raw_entity in reader:
        entity = context.make("LegalEntity")
        entity_type = raw_entity["TIPO DE PESSOA"]
        entity.id = context.make_id(raw_entity["CPF OU CNPJ DO SANCIONADO"])
        if entity_type == "F":
            entity.add_schema("Person")
        elif entity_type == "J":
            entity.add_schema("Company")
        elif entity_type == "":
            pass
        else:
            context.log.error("Unknown entity type", tipo=entity_type)
            continue
        entity.add("name", raw_entity["NOME DO SANCIONADO"])
        entity.add("taxNumber", raw_entity["CPF OU CNPJ DO SANCIONADO"])
        entity.add("country", "br")
        entity.add("topics", "debarment")

        sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
        sanction.add("reason", raw_entity["FUNDAMENTAÇÃO LEGAL"], lang="por")
        context.emit(entity)
        context.emit(sanction)
