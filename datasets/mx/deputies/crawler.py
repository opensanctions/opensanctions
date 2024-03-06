import re
import requests

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

def crawl_item(input_dict: dict, context: Context):

    entity = context.make("Person")
    entity.id = context.make_id(input_dict["NombreCompleto"])

    entity.add("lastName", input_dict.pop("Nombre"))
    entity.add("firstName", input_dict.pop("PrimerApellido"))
    entity.add("name", input_dict.pop("NombreCompleto"))

    if input_dict["Telefono"] is not None:
        entity.add("phone", "+52"+input_dict.pop("Telefono"))

    entity.add("email", input_dict.pop("Correo"))

    position = h.make_position(context, "Member of the Chamber of Deputies", country="mx")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
            context,
            entity,
            position,
            True,
            categorisation=categorisation,
        )

    if occupancy is None:
        return

    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)
    

def crawl(context: Context):

    HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://web.diputados.gob.mx/',
    'Content-Type': 'application/json',
    'Origin': 'https://web.diputados.gob.mx',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    }

    json_data = {
    'operationName': None,
    'variables': {},
    'query': '{\n  allDiputados {\n    Oid\n    Nombre\n    PrimerApellido\n    SegundoApellido\n    NombreCompleto\n    Estado\n    Partido\n    Distrito\n    Legislacion\n    PrimerApellido\n    CabeceraMunicipal\n    Suplente\n    id_dip\n    IdDiputado\n    Correo\n    Telefono\n    TipoEleccion\n    Licencia\n    __typename\n}\n}\n',
    }

    # We are going to query the graphql endpoint to retrive the data and then process each item
    response = requests.post('https://micrositios.diputados.gob.mx:4001/graphql', headers=HEADER, json=json_data).json()

    for item in response["data"]["allDiputados"]:
        crawl_item(item, context)

