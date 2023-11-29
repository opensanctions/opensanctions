from zavod import Context
from zipfile import ZipFile
import csv
import io
import re
from typing import List

SITE_URL = "https://portaldatransparencia.gov.br/download-de-dados/ceis"
HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }


def get_csv_url(context: Context) -> str:
    doc = context.fetch_html(SITE_URL, headers=HEADERS)
    path = '//script'
    date_pattern = re.compile(r'"ano"\s*:\s*"(\d+)",\s*"mes"\s*:\s*"(\d+)",\s*"dia"\s*:\s*"(\d+)"')
    for script in doc.xpath(path):
        if script.text:
            match = date_pattern.search(script.text)
            if match:
                year, month, day = match.groups()
                return SITE_URL + f"/{year}{month}{day}"

    raise ValueError("Data URL not found")


def get_data(csv_url: str, context: Context) -> List[dict]:
    response = context.fetch_response(csv_url, headers=HEADERS)
    zip_file = ZipFile(io.BytesIO(response.content))
    file_name = zip_file.namelist()[0]

    csv_str = zip_file.read(file_name).decode('iso-8859-1')
    lines = csv_str.splitlines()

    reader = csv.DictReader(lines, delimiter=';')

    return [row for row in reader]


def create_entities(data: List[dict], context: Context) -> None:

    for raw_entity in data:
        if raw_entity['TIPO DE PESSOA'] == 'F':
            entity = context.make('Person')
            entity.id = context.make_id(raw_entity['CPF OU CNPJ DO SANCIONADO'])
            entity.add('name', raw_entity['NOME DO SANCIONADO'])
            context.emit(entity, target=True)

        if raw_entity['TIPO DE PESSOA'] == 'J':
            entity = context.make('Company')
            entity.id = context.make_id(raw_entity['CPF OU CNPJ DO SANCIONADO'])
            entity.add('name', raw_entity['NOME DO SANCIONADO'])
            context.emit(entity, target=True)


def crawl(context: Context):
    csv_url = get_csv_url(context)
    data = get_data(csv_url, context)
    create_entities(data, context)
