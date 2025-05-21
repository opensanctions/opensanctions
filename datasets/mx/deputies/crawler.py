import orjson

from zavod import Context, helpers as h
from zavod.shed import zyte_api
from zavod.shed.zyte_api import ZyteAPIRequest
from zavod.stateful.positions import categorise


def crawl_item(input_dict: dict, context: Context):
    entity = context.make("Person")
    parts = [input_dict["NombreCompleto"]]
    state = input_dict.pop("Estado")
    if state:
        parts.append(state)
    id = context.make_slug(parts)
    entity.id = id
    last_name = (
        (input_dict.pop("PrimerApellido") or "")
        + " "
        + (input_dict.pop("SegundoApellido") or "")
    ).strip()
    h.apply_name(
        entity,
        full=input_dict.pop("NombreCompleto"),
        first_name=input_dict.pop("Nombre"),
        last_name=last_name,
    )

    if input_dict["Telefono"] is not None:
        entity.add("phone", "+52" + input_dict.pop("Telefono"))

    entity.add("email", input_dict.pop("Correo"))
    entity.add("political", input_dict.pop("Partido"))

    position = h.make_position(
        context, "Member of the Chamber of Deputies of Mexico", country="mx"
    )
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

    context.emit(entity)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(
        input_dict,
        [
            "Oid",
            "Distrito",
            "Legislacion",
            "CabeceraMunicipal",
            "Suplente",
            "id_dip",
            "IdDiputado",
            "__typename",
            "Licencia",
        ],
    )


def crawl(context: Context):
    json_data = {
        "operationName": None,
        "variables": {},
        "query": """{ allDiputados
          {  Oid
             Nombre
             PrimerApellido
             SegundoApellido
             NombreCompleto
             Estado
             Partido
             Distrito
             Legislacion
             PrimerApellido
             CabeceraMunicipal
             Suplente
             id_dip
             IdDiputado
             Correo
             Telefono
             TipoEleccion
             Licencia
             __typename
          }
        }""",
    }

    # We are going to query the graphql endpoint to retrieve the data and then process each item
    zyte_result = zyte_api.fetch(
        context,
        ZyteAPIRequest(
            url="https://micrositios.diputados.gob.mx:4001/graphql",
            method="POST",
            body=orjson.dumps(json_data),
            headers={"Content-Type": "application/json"},
            geolocation="MX",
        ),
    )
    results = orjson.loads(zyte_result.response_text)

    for item in results["data"]["allDiputados"]:
        crawl_item(item, context)
