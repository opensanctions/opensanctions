import csv
from typing import Dict, Any
from zipfile import ZipFile
from datetime import datetime, timedelta

from zavod import Context
from zavod import helpers as h
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
    for i in range(13):
        prev = datetime.now() - timedelta(days=i * 28)
        url = context.data_url + f"/{prev.strftime('%Y%m')}"
        resp = context.http.head(url, allow_redirects=True)
        if resp.status_code == 200:
            return url
    raise ValueError("Data URL not found")


def create_entity(raw_entity: Dict[str, Any], context: Context) -> None:
    """
    Creates entities from the data fetched from the website.

    :param data: The data fetched from the website as a list of dicts.
    :param context: The context object.
    """
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
        start_date=raw_entity["Data_Início_Exercício"],
        end_date=raw_entity["Data_Fim_Exercício"],
        categorisation=categorisation,
    )

    if occupancy is not None:
        context.emit(person)
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
    path = context.fetch_resource("source.zip", csv_url)
    work_dir = path.parent / "files"
    work_dir.mkdir(exist_ok=True)
    with ZipFile(path) as zip_file:
        for file_name in zip_file.namelist():
            context.log.info(f"Extracting {file_name}")
            file_path = zip_file.extract(file_name, work_dir)
            with open(file_path, "r", encoding="iso-8859-1") as fh:
                reader = csv.DictReader(fh, delimiter=";")
                for row in reader:
                    create_entity(row, context)
