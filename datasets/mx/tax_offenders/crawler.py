import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h

ALLOW_FILES = [
    "Cancelados",
    "ReducciÃ³n de multas (ArtÃ\xadculo 74 del CÃ³digo Fiscal de la FederaciÃ³n)",
    "Condonados de concurso mercantil (ArtÃ\xadculo 146B del CÃ³digo Fiscal de la FederaciÃ³n)",
    "ReducciÃ³n de recargos (ArtÃ\xadculo 21 del CÃ³digo Fiscal de la FederaciÃ³n)",
    "Condonados por decreto (Del 22 de enero y 26 de marzo de 2015)",
    "Condonados del 01 de enero de 2007 al 04 de mayo de 2015",
    "Cancelados ArtÃ\xadculo 146A del 01 de enero de 2007 al 04 de mayo de 2015",
    "Retorno de inversiones",
    "Exigibles",
    "Firmes",
    "No localizados",
    "Sentencias",
]

DENY_FILES = [
    "Documento tÃ©cnico y normativo",
    "Certificado de Sello Digital (CSD) sin efectos",
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
    schema = context.lookup_value("person_type", input_dict.get("person_type"))

    if not schema:
        context.log.info(input_dict.get("person_type"))
        return
    input_dict.pop("person_type")

    entity = context.make(schema)
    entity.id = context.make_id(input_dict.get("RFC"), input_dict.get("name"))
    entity.add("name", input_dict.pop("name"))
    entity.add("taxNumber", input_dict.pop("RFC"))
    entity.add("topics", "crime.fin")
    entity.add("country", "mx")

    reason = input_dict.pop("reason")
    start_date = input_dict.pop("start_date")
    sanction = h.make_sanction(context, entity, key=f"{reason}-{start_date}")
    sanction.add("reason", reason)
    if input_dict.get("start_date"):
        h.apply_date(sanction, "startDate", start_date)
    if input_dict.get("listing_date"):
        h.apply_date(sanction, "listingDate", input_dict.pop("listing_date"))
    sanction.add("authority", input_dict.pop("authority"))
    if input_dict.get("value"):
        sanction.add("description", "Value: " + input_dict.pop("value"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(input_dict)


def get_files_urls(context: Context):
    response = context.fetch_html(context.data_url)

    for a in response.findall(".//a"):
        if a.text_content() in ALLOW_FILES:
            yield a.text_content(), a.get("href")
        elif a.text_content() in DENY_FILES:
            continue
        else:
            context.log.warning(
                "Unkown file found", label=a.text_content(), url=a.get("href")
            )


def crawl(context: Context):
    for label, url in get_files_urls(context):
        fname = url.split("/")[-1]
        source_file = context.fetch_resource(fname, url)
        context.export_resource(source_file, CSV, context.SOURCE_TITLE)

        with open(source_file, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for item in reader:
                # Each csv has a slightly different name for each attribute
                # so we are going to normalize them
                replace_key(item, context)
                crawl_item(item, context)
