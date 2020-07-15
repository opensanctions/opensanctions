import re
from pprint import pprint  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

from opensanctions.util import normalize_country

SPLITS = r"(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)"  # noqa
SOURCE = "https://www.worldbank.org/en/projects-operations/procurement/debarred-firms"  # noqa


def clean_date(date):
    try:
        dt = datetime.strptime(date, "%d-%b-%Y")
        return dt.date().isoformat()
    except Exception:
        pass


def clean_name(text):
    text = text.replace("M/S", "MS")
    parts = re.split(SPLITS, text, re.I)
    names = []
    keep = True
    for part in parts:
        if part is None:
            continue
        if keep:
            names.append(part)
            keep = False
        else:
            keep = True

    clean_names = []
    for name in names:
        if "*" in name:
            name, _ = name.rsplit("*", 1)
        # name = re.sub(r'\* *\d{1,4}$', '', name)
        name = name.strip(")").strip("(").strip(",")
        name = name.strip()
        clean_names.append(name)
    return clean_names


def fetch(context, data):
    url = context.params.get("url")
    apikey = context.params.get("apikey")
    response = context.http.get(url, headers={"apikey": apikey})
    for ent in response.json["response"]["ZPROCSUPP"]:
        context.emit(data=ent)


def parse(context, data):
    emitter = EntityEmitter(context)
    entity = emitter.make("LegalEntity")

    name = data.get("SUPP_NAME")
    ent_id = data.get("SUPP_ID")
    reason = data.get("DEBAR_REASON")
    country = data.get("COUNTRY_NAME")
    city = data.get("SUPP_CITY")
    address = data.get("SUPP_ADDR")
    start_date = data.get("DEBAR_FROM_DATE")
    end_date = data.get("DEBAR_TO_DATE")

    entity.make_id(name, ent_id, country)
    names = clean_name(name)
    entity.add("name", names[0])
    entity.add("address", address)
    entity.add("address", city)
    entity.add("country", normalize_country(country))
    for name in names[1:]:
        entity.add("alias", name)

    sanction = emitter.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("authority", "World Bank Debarrment")
    sanction.add("program", reason)
    sanction.add("startDate", clean_date(start_date))
    sanction.add("endDate", clean_date(end_date))
    sanction.add("sourceUrl", SOURCE)
    emitter.emit(entity)
    emitter.emit(sanction)

    emitter.finalize()
