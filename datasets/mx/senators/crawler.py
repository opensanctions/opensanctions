import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html, fetch_json


def get_json_url(context: Context) -> str:
    """
    Fetches the JSON URL from the main page.
    The URL for the data depends on which legislature it is at.
    For example, as of today we are at the 65th legislature and thus
    the link for the raw data will be https://www.senado.gob.mx/65/datosAbiertos/senadoresDatosAb.json

    :param context: The context object.

    :return: The URL for the JSON file.
    """
    redirect_xpath = ".//meta[@http-equiv='Refresh']"
    doc = fetch_html(
        context,
        context.data_url,
        redirect_xpath,
        html_source="httpResponseBody",
        geolocation="MX",
        cache_days=1,
    )
    main_website = doc.find(redirect_xpath).get("content")
    url_pattern = r"url=\b(\d{2})/"
    match = re.search(url_pattern, main_website)

    if match is None:
        context.log.error("Senators URL not found")
        return None

    return f"https://www.senado.gob.mx/{match.group(1)}/datosAbiertos/senadoresDatosAb.json"


def crawl_item(input_dict: dict, position, categorisation, context: Context):
    """
    Creates an entity, a position and a occupancy from the raw data.

    :param input_dict: Data dict extracted from source.
    :param position: The position it holds
    :param categorisation: The categorisation of the position
    :param context: The context object.
    """

    entity = context.make("Person")
    entity.id = context.make_id(
        input_dict["Estado"], input_dict["Apellidos"], input_dict["Nombre"]
    )
    h.apply_name(
        entity,
        first_name=input_dict.pop("Nombre"),
        last_name=input_dict.pop("Apellidos"),
    )

    gender = (
        "male"
        if input_dict["Sexo"] == "Hombre"
        else "female" if input_dict["Sexo"] == "Mujer" else "other"
    )
    input_dict.pop("Sexo")
    entity.add("gender", gender)

    entity.add("email", input_dict.pop("correo"))
    entity.add("address", input_dict.pop("direccion"))
    entity.add("political", input_dict.pop("Fraccion"))
    entity.add("website", input_dict.pop("url_sitio"))

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    context.emit(entity)
    context.emit(occupancy)
    context.audit_data(
        input_dict,
        ignore=[
            "idSenador",
            "Legislatura",
            "Estado",
            "tipoEleccion",
            "Suplente",
            "estadoOrigen",
            "estatus",
            "id",
            "twitter",
            "instagram",
            "telefono",
            "extension",
        ],
    )


def crawl(context: Context):
    """
    Entrypoint to the crawler.

    The crawler works by first finding the url for the senators data,
    then fetching the data from the URL as a JSON.
    Finally we create the entities.

    :param context: The context object.
    """
    senators_url = get_json_url(context)

    senators = fetch_json(
        context,
        senators_url,
        expected_charset=None,
        geolocation="MX",
        cache_days=1,
    )

    # We first define the Mexico Senator Position
    position = h.make_position(context, "Member of the Senate of Mexico", country="mx")
    categorisation = categorise(context, position, is_pep=True)
    context.emit(position)

    for item in senators:
        crawl_item(item, position, categorisation, context)
