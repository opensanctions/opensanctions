import json
from typing import Any, Dict
from urllib.parse import urljoin
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

COUNTRIES = {
    0: "RU",
    1: "RU",
    2: "BY",
    3: "UA",
    4: None,
    5: "UA-CRI",
    None: None,
}


def clean_row(row: Dict[str, Any]) -> Dict[str, str]:
    data = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, int):
            data[k] = v
            continue
        v = v.strip()
        if v in ("", "-"):
            continue
        data[k] = v
    return data


def parse_date(date):
    if date is not None:
        date = date.replace(" ", "")
    dates = set()
    for part in multi_split(date, [",", "\n", ";"]):
        dates.update(h.parse_date(part, ["%d.%m.%Y", "dd.%m.%Y"]))
    return dates


def json_resource(context: Context, url, name):
    full_url = urljoin(url, name)
    path = context.fetch_resource(f"{name}.json", full_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        return json.load(fh)


def crawl_person(context: Context) -> None:
    data = json_resource(context, context.dataset.data.url, "person")
    for row in data["data"]:
        row = clean_row(row)
        person_id = row.pop("person_id")
        name_en = row.pop("name_en", None)
        name_ru = row.pop("name_ru", None)
        name_uk = row.pop("name_uk", None)
        name = name_en or name_ru or name_uk
        entity = context.make("Person")
        entity.id = context.make_slug("person", person_id, name)
        entity.add("name", name)
        entity.add("alias", name_ru)
        entity.add("alias", name_uk)
        entity.add("birthDate", parse_date(row.pop("date_bd", None)))
        url = "https://sanctions.nazk.gov.ua/sanction-person/%s/"
        entity.add("sourceUrl", url % person_id)
        if row.get("city_bd_en") != "N/A":
            entity.add("birthPlace", row.pop("city_bd_en", None))
            entity.add("birthPlace", row.pop("city_bd_ru", None))
            entity.add("birthPlace", row.pop("city_bd_uk", None))
        entity.add("position", row.pop("position_en", None))
        entity.add("position", row.pop("position_ru", None))
        entity.add("position", row.pop("position_uk", None))
        entity.add("notes", row.pop("reasoning_en", None))
        entity.add("notes", row.pop("reasoning_ru", None))
        entity.add("notes", row.pop("reasoning_uk", None))

        country = row.get("country", None)
        entity.add("country", COUNTRIES[country])
        entity.add("topics", "sanction")
        context.emit(entity, target=True)
        # h.audit_data(row)


def crawl_company(context: Context) -> None:
    data = json_resource(context, context.dataset.data.url, "company")
    for row in data["data"]:
        row = clean_row(row)
        # context.pprint(row)
        company_id = row.pop("company_id")
        name = row.pop("name", None)
        entity = context.make("Organization")
        entity.id = context.make_slug("company", company_id, name)
        if entity.id is None:
            entity.id = context.make_slug(
                "company",
                company_id,
                row.get("ogrn"),
                strict=False,
            )
        entity.add("name", name)
        entity.add("innCode", row.pop("inn", None))
        entity.add_cast("Company", "ogrnCode", row.pop("ogrn", None))

        country = row.get("country", None)
        entity.add("country", COUNTRIES[country])
        entity.add("topics", "sanction")
        context.emit(entity, target=True)


def crawl(context: Context) -> None:
    crawl_person(context)
    crawl_company(context)
