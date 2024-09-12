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


def replace_key(d: dict, context: Context) -> dict:
    """
    This function returns the dictionary d with the keys replaced using the
    column lookup. If there are two matches, then it raises an error
    replaced by the new key
    """

    keys = list(d.keys())

    for key in keys:
        new_key = context.lookup_value("columns", key)
        if new_key:
            if new_key in d:
                context.log.error("Multiple matches in the same dictionary")
            else:
                d[new_key] = d.pop(key)
    return d


def crawl_item(input_dict: dict, context: Context):

    schema = "Person" if input_dict.pop("person_type") == "F" else "Company"

    entity = context.make(schema)
    entity.id = context.make_id(input_dict.get("RFC"), input_dict.get("name"))
    entity.add("name", input_dict.pop("name"))
    entity.add("taxNumber", input_dict.pop("RFC"))
    entity.add("topics", "crime.fin")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", input_dict.pop("reason"))
    if input_dict.get("start_date"):
        h.apply_date(sanction, "startDate", input_dict.pop("start_date"))
    if input_dict.get("listing_date"):
        h.apply_date(sanction, "listingDate", input_dict.pop("listing_date"))
    sanction.add("authority", input_dict.pop("authority"))
    sanction.add("description", "Value: " + input_dict.pop("value"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):

    for url in URLs:
        fname = url.split("/")[-1]
        source_file = context.fetch_resource(fname, url)
        with open(source_file, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for item in reader:
                # Each csv has a slightly different name for each attribute
                # so we are going to normalize them
                replace_key(item, context)
                crawl_item(item, context)
