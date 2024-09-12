import csv
from typing import List

from zavod import Context, helpers as h

URLs = [
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Cancelados.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/ReduccionArt74CFF.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Condonadosart146BCFF.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Condonadosart21CFF.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/CondonadosporDecreto.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Condonados_07_15.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Cancelados_07_15.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Retornoinversiones.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Exigibles.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Firmes.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/No%20localizados.csv",
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Sentencias.csv",
]


def replace_key(d: dict, possible_old_keys: List[str], new_key: str) -> dict:
    """
    This function returns the dictionary d with the first key found in possible_old_keys
    replaced by the new key
    """

    for possible_old_key in possible_old_keys:
        if possible_old_key in d:
            d[new_key] = d.pop(possible_old_key)
            break
    return d


def crawl_item(input_dict: dict, context: Context):

    schema = "Person" if input_dict.pop("person_type") == "F" else "Company"

    entity = context.make(schema)
    entity.id = context.make_id(input_dict.get("RFC"), input_dict.get("name"))
    entity.add("name", input_dict.pop("name"))
    entity.add("taxNumber", input_dict.pop("RFC"))
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", input_dict.pop("reason"))
    if input_dict.get("start_date"):
        h.apply_date(sanction, "start_date", input_dict.pop("start_date"))
    sanction.add("authority", input_dict.pop("authority"))
    sanction.add("description", "Value: " + input_dict.pop("value"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(
        input_dict,
        ignore=[
            "Fecha de publicación (Con monto de acuerdo a la Ley de Transparencia",
            "FECHA DE PUBLICACIÓN",
        ],
    )


def crawl(context: Context):

    for url in URLs:
        fname = url.split("/")[-1]
        source_file = context.fetch_resource(fname, url)
        with open(source_file, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for item in reader:
                # Each csv has a slightly different name for each attribute
                # so we are going to normalize them
                replace_key(
                    item,
                    [
                        "RAZÓN SOCIAL",
                        "NOMBRE, DENOMINACIÓN O RAZÓN SOCIAL",
                        "Contribuyente",
                    ],
                    "name",
                )
                replace_key(
                    item,
                    [
                        "TIPO PERSONA",
                        "TIPO DE PERSONA",
                        "Tipo de persona",
                        "Tipo persona",
                    ],
                    "person_type",
                )
                replace_key(
                    item, ["SUPUESTO", " Motivo de condonación ", "Motivo"], "reason"
                )
                replace_key(
                    item,
                    [
                        " Importe pesos ",
                        "MONTO ",
                        "MONTO",
                        " MONTO ",
                        " Importe condonado ",
                    ],
                    "value",
                )
                replace_key(
                    item,
                    [
                        "FECHA DE CANCELACIÓN",
                        "FECHA DE AUTORIZACIÓN",
                        "FECHAS DE PRIMERA PUBLICACION",
                        "Año",
                    ],
                    "start_date",
                )
                replace_key(
                    item, ["ENTIDAD FEDERATIVA", "Entidad Federativa"], "authority"
                )

                crawl_item(item, context)
