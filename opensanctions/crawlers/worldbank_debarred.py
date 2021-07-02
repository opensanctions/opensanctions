import re
from pprint import pprint  # noqa

from opensanctions.helpers import make_address
from opensanctions.util import date_formats, DAY

SPLITS = r"(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)"  # noqa


def parse_date(text):
    return date_formats(text, [("%d-%b-%Y", DAY)])


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


def crawl(context):
    url = context.dataset.data.url
    headers = {"apikey": context.dataset.data.api_key}
    # print(url)
    res = context.http.get(url, headers=headers, timeout=240)
    for data in res.json()["response"]["ZPROCSUPP"]:
        # pprint(data)
        entity = context.make("LegalEntity")
        name = data.get("SUPP_NAME")
        ent_id = data.get("SUPP_ID")
        entity.make_slug(ent_id)
        names = clean_name(name)
        entity.add("name", names[0])
        entity.add("country", data.get("COUNTRY_NAME"))
        for name in names[1:]:
            entity.add("alias", name)

        address = make_address(
            context,
            street=data.get("SUPP_ADDR"),
            city=data.get("SUPP_CITY"),
            country=data.get("COUNTRY_NAME"),
            key=entity.id,
        )
        if address is not None:
            context.emit(address)
            entity.add("addressEntity", address)

        sanction = context.make_sanction(entity)
        sanction.add("program", data.get("DEBAR_REASON"))
        sanction.add("startDate", parse_date(data.get("DEBAR_FROM_DATE")))
        sanction.add("endDate", parse_date(data.get("DEBAR_TO_DATE")))
        context.emit(entity, target=True)
        context.emit(sanction)
