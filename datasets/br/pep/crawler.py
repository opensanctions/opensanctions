import csv
from typing import Dict, Any
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.extract.zyte_api import fetch_resource, fetch_html

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
    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator="//select[@id='links-meses']",
        geolocation="BR",
        absolute_links=True,
    )
    return h.xpath_string(doc, "//a[contains(@href, '/download-de-dados/pep/')]/@href")


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
    person.add("citizenship", "br")

    position_name = f"{raw_entity['Descrição_Função']}, {raw_entity['Nome_Órgão']}"
    position = h.make_position(context, position_name, country="br")
    categorisation = categorise(context, position, default_is_pep=True)

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


def crawl(context: Context) -> None:
    """
    Entrypoint to the crawler.

    The crawler works by first fetching the CSV URL from the main page. This is necessary because the CSV URL is
    dynamically generated and changes every day. The CSV URL is then used to download the CSV file, which is then
    parsed and the entities are created.

    :param context: The context object.
    """
    csv_url = get_csv_url(context)
    # The portal serves the ZIP with the legacy IIS content type
    # "application/x-zip-compressed" rather than the canonical "application/zip".
    _, _, _, path = fetch_resource(
        context, "source.zip", csv_url, "application/x-zip-compressed", geolocation="BR"
    )
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
