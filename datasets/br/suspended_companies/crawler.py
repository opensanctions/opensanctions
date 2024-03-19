import io
import re
import csv
from typing import List
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h


def get_csv_url(context: Context) -> str:
    """
    Fetches the CSV URL from the main page.
    The CSV URL is dynamically generated and changes every day
    and is created by concatenating the base url (the url in the metadata) with the date in the format YYYYMMDD.

    The date is in a script tag in the main page.

    :param context: The context object.

    :return: The URL of the CSV file.
    """

    doc = context.fetch_html(context.data_url)
    path = "//script"
    date_pattern = re.compile(
        r'"ano"\s*:\s*"(\d+)",\s*"mes"\s*:\s*"(\d+)",\s*"dia"\s*:\s*"(\d+)"'
    )
    for script in doc.xpath(path):
        if script.text:
            match = date_pattern.search(script.text)
            if match:
                year, month, day = match.groups()
                return context.data_url + f"/{year}{month}{day}"

    raise ValueError("Data URL not found")


def get_data(csv_url: str, context: Context) -> List[dict]:
    """
    Fetches the CSV file as a zip from the website decompresses it and parses it using the csv library
    and returns the data as a list of dicts.

    :param csv_url: The URL of the CSV file.
    :param context: The context object.

    :return: The data fetched from the website as a list of dicts.
    """
    response = context.fetch_response(csv_url)
    zip_file = ZipFile(io.BytesIO(response.content))
    file_name = zip_file.namelist()[0]

    csv_str = zip_file.read(file_name).decode("iso-8859-1")
    lines = csv_str.splitlines()

    reader = csv.DictReader(lines, delimiter=";")

    return [row for row in reader]


def create_entities(data: List[dict], context: Context) -> None:
    """
    Creates entities from the data fetched from the website.

    :param data: The data fetched from the website as a list of dicts.
    :param context: The context object.
    """

    for raw_entity in data:
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

        sanction = h.make_sanction(context, entity)
        sanction.add("program", "Brazil disreputed and sanctioned companies")
        sanction.add("reason", raw_entity["FUNDAMENTAÇÃO LEGAL"], lang="por")
        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    """
    Entrypoint to the crawler.

    The crawler works by first fetching the CSV URL from the main page. This is necessary because the CSV URL is
    dynamically generated and changes every day. The CSV URL is then used to download the CSV file, which is then
    parsed and the entities are created.

    :param context: The context object.
    """
    csv_url = get_csv_url(context)
    data = get_data(csv_url, context)
    create_entities(data, context)
