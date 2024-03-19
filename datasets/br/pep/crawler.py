import io
import re
import csv
from typing import List
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from datetime import datetime

from zavod.logic.pep import categorise

# 1: CPF
# 2: PEP_Name
# 3: Acronym_Function
# 4: Function_Description
# 5: Function_Level
# 6: Organization_Name
# 7: Exercise_Start_Date
# 8: Exercise_End_Date
# 9: End_Date_Grace


def parse_date(date_string):
    try:
        return datetime.strptime(date_string, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def get_csv_url(context: Context) -> str:
    """
    Fetches the CSV URL from the main page.
    The CSV URL is dynamically generated and changes every day
    and is created by concatenating the base url
    (the url in the metadata) with the date in the format YYYYMM (it doesn't include the day).

    The date is in a script tag in the main page.

    :param context: The context object.

    :return: The URL of the CSV file.
    """
    doc = context.fetch_html(context.data_url, cache_days=1)
    path = "//script"
    date_pattern = re.compile(
        r'"ano"\s*:\s*"(\d+)",\s*"mes"\s*:\s*"(\d+)",\s*"dia"\s*:\s*'
    )
    for script in doc.xpath(path):
        if script.text:
            match = date_pattern.search(script.text)
            if match:
                # we can ignore the day since it won't be used to build the url
                year, month = match.groups()
                return context.data_url + f"/{year}{month}"

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
    path = context.fetch_resource("source.zip", csv_url)
    zip_file = ZipFile(path)
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
        person = context.make("Person")
        # We can't use only the CPF (tax number) as an id because here it comes anonimized (12345678910 -> ***456789**)
        person.id = context.make_id(raw_entity["CPF"] + raw_entity["Nome_PEP"])
        person.add("name", raw_entity["Nome_PEP"])
        person.add("taxNumber", raw_entity["CPF"])

        position_name = f'{raw_entity["Descrição_Função"]}, {raw_entity["Nome_Órgão"]}'
        position = h.make_position(context, position_name, country="br")
        categorisation = categorise(context, position, is_pep=True)

        if not categorisation.is_pep:
            return

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,
            start_date=parse_date(raw_entity["Data_Início_Exercício"]),
            end_date=parse_date(raw_entity["Data_Fim_Exercício"]),
            categorisation=categorisation,
        )

        if occupancy is not None:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)


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
