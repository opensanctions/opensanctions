import re
import requests

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def get_json_url(context: Context) -> str:
    """
    Fetches the JSON URL from the main page.
    The URL for the data depends on which legislature it is at.
    For example, as of today we are at the 65th legislature and thus
    the link for the raw data will be https://www.senado.gob.mx/65/datosAbiertos/senadoresDatosAb.json

    :param context: The context object.

    :return: The URL for the JSON file.
    """

    # the ssl verification for the site is failing
    main_website = requests.get(context.data_url, verify=False).text
    url_pattern = (
        r"https://www\.senado\.gob\.mx/\d+/datosAbiertos/senadoresDatosAb\.json"
    )

    matches = re.search(url_pattern, main_website)

    if matches is None:
        context.log.error("Senators URL not found")
        return None

    return matches.group()


def crawl_item(input_dict: dict, position, categorisation, context: Context):
    """
    Creates an entity, a position and a occupancy from the raw data.

    :param input_dict: Data dict extracted from source.
    :param position: The position it holds
    :param categorisation: The categorisation of the position
    :param context: The context object.
    """

    entity = context.make("Person")
    entity.id = context.make_id(input_dict["Apellidos"], input_dict["Nombre"])

    entity.add("lastName", input_dict.pop("Apellidos"))
    entity.add("firstName", input_dict.pop("Nombre"))

    gender = (
        "male"
        if input_dict["Sexo"] == "Hombre"
        else "female" if input_dict["Sexo"] == "Mujer" else "other"
    )

    input_dict.pop("Sexo")

    entity.add("gender", gender)
    entity.add("email", input_dict.pop("correo"))
    entity.add("address", input_dict.pop("direccion"))

    # they can have multiple phone numbers, which will be
    # differed by the extensions
    # sometimes the extension is represented like "3561, 5507, 5139"
    # and other times like "3561, 5507 y 5139"
    # so we extract all occurrences 4 digits from the string
    extensions = re.findall(r"\b\d{4}\b", input_dict.pop("extension"))
    base_number = input_dict.pop("telefono").replace(" ", "")

    for extension in extensions:
        entity.add("phone", "+52" + base_number + extension)

    entity.add("website", input_dict.pop("url_sitio"))

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    context.emit(entity, target=True)
    context.emit(occupancy)


def crawl(context: Context):
    """
    Entrypoint to the crawler.

    The crawler works by first finding the url for the senators data,
    then fetching the data from the URL as a JSON.
    Finally we create the entities.

    :param context: The context object.
    """
    senators_url = get_json_url(context)

    if senators_url is None:
        return

    response = context.fetch_json(senators_url)

    # We first define the Mexico Senator Position
    position = h.make_position(context, "Member of the Senate of Mexico", country="mx")
    categorisation = categorise(context, position, is_pep=True)
    context.emit(position)

    for item in response:
        crawl_item(item, position, categorisation, context)
