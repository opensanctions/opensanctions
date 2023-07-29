import re

from zavod import Context
from zavod import helpers as h

SPLITS = r"(a\.k\.a\.?|aka|f/k/a|also known as|\(formerly |, also d\.b\.a\.|\(currently (d/b/a)?|d/b/a|\(name change from|, as the successor or assign to)"  # noqa

FORMATS = ("%d-%b-%Y",)


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


def crawl(context: Context):
    url = context.data_url
    headers = {"apikey": context.dataset.data.api_key}
    data = context.fetch_json(url, headers=headers)
    # TODO write this out to a source.json
    for data in data["response"]["ZPROCSUPP"]:
        # context.inspect(data)
        entity = context.make("LegalEntity")
        name = data.get("SUPP_NAME")
        ent_id = data.get("SUPP_ID")
        entity.id = context.make_slug(ent_id)
        names = clean_name(name)
        entity.add("name", names[0])
        entity.add("topics", "debarment")
        entity.add("country", data.get("COUNTRY_NAME"))
        for name in names[1:]:
            entity.add("alias", name)

        address = h.make_address(
            context,
            street=data.get("SUPP_ADDR"),
            city=data.get("SUPP_CITY"),
            country=data.get("COUNTRY_NAME"),
            key=entity.id,
        )
        h.apply_address(context, entity, address)

        sanction = h.make_sanction(context, entity)
        sanction.add("program", data.get("DEBAR_REASON"))
        sanction.add("startDate", h.parse_date(data.get("DEBAR_FROM_DATE"), FORMATS))
        sanction.add("endDate", h.parse_date(data.get("DEBAR_TO_DATE"), FORMATS))
        context.emit(entity, target=True)
        context.emit(sanction)
