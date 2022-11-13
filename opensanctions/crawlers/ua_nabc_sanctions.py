import json
from typing import Any, Dict, Union
from urllib.parse import urljoin
from pantomime.types import JSON

from opensanctions.core import Context, Entity
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

TRACK_COUNTRIES = ["ua", "us", "au", "ca", "ch", "es", "gb", "jp", "nz", "pl"]


def clean_row(row: Dict[str, Any]) -> Dict[str, Union[str, Dict[str, str]]]:
    data = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, int):
            data[k] = v
            continue
        if isinstance(v, dict):
            data[k] = clean_row(v)
            continue
        if isinstance(v, list):
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


def url_split(urls):
    urls = []
    for url in multi_split(urls, [";", "\n"]):
        if len(url) > 5:
            urls.append(url)
    return urls


def json_resource(context: Context, url, name):
    full_url = urljoin(url, name)
    path = context.fetch_resource(f"{name}.json", full_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        return json.load(fh)


def crawl_common(context: Context, entity: Entity, row: Dict[str, Any]):
    entity.add("topics", "sanction")
    country = row.pop("country", None)
    entity.add("country", COUNTRIES.get(country, country))
    entity.add("keywords", row.pop("category", None))
    entity.add("keywords", row.pop("subcategory_1", None))
    entity.add("keywords", row.pop("subcategory_2", None))
    entity.add("keywords", row.pop("subcategory_3", None))

    entity.add("website", url_split(row.pop("link", "")))
    entity.add("innCode", row.pop("itn", None))
    entity.add("address", row.pop("address_ru", None))
    entity.add("address", row.pop("address_uk", None))
    entity.add("address", row.pop("address_en", None))

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("sanctions_ua_date", None))
    if row.pop("sanctions_ua") == 1:
        sanction.add("status", "active")

    sanction.add("sourceUrl", url_split(row.pop("url_ua", "")))

    sanction.add("reason", row.pop("reasoning_en", None))
    sanction.add("reason", row.pop("reasoning_ru", None))
    sanction.add("reason", row.pop("reasoning_uk", None))

    row.pop("status", None)
    row.pop("synchron", None)
    row.pop("top_50", None)
    row.pop("link_archive", None)
    row.pop("sort_order", None)

    row.pop("relations_person", None)
    row.pop("relations_company", None)

    for cc in TRACK_COUNTRIES:
        row.pop(f"sanctions_{cc}", None)
        row.pop(f"sanctions_{cc}_date", None)
        row.pop(f"url_{cc}", None)


def crawl_person(context: Context) -> None:
    data = json_resource(context, context.source.data.url, "person")
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
        entity.add("deathDate", parse_date(row.pop("date_dead", None)))
        url = f"https://sanctions.nazk.gov.ua/sanction-person/{person_id}/"
        entity.add("sourceUrl", url)
        if row.get("city_bd_en") != "N/A":
            entity.add("birthPlace", row.pop("city_bd_en", None))
            entity.add("birthPlace", row.pop("city_bd_ru", None))
            entity.add("birthPlace", row.pop("city_bd_uk", None))
        entity.add("position", row.pop("position_en", None))
        entity.add("position", row.pop("position_ru", None))
        entity.add("position", row.pop("position_uk", None))

        # TODO: emit image
        photo_url = row.pop("photo_name", None)

        crawl_common(context, entity, row)
        context.emit(entity, target=True)
        h.audit_data(row)


def crawl_company(context: Context) -> None:
    data = json_resource(context, context.source.data.url, "company")
    for row in data["data"]:
        row = clean_row(row)
        company_id = row.pop("company_id")
        name_en = row.pop("name_en", None)
        name = row.pop("name", None) or name_en
        entity = context.make("Organization")
        entity.id = context.make_slug("company", company_id, name)
        if entity.id is None:
            entity.id = context.make_slug(
                "company",
                company_id,
                row.pop("ogrn", None),
                strict=False,
            )
        entity.add("name", name)
        entity.add("name", name_en)
        entity.add("name", row.pop("name_uk", None))
        entity.add("name", row.pop("name_ru", None))
        entity.add("innCode", row.pop("inn", None))
        url = f"https://sanctions.nazk.gov.ua/en/sanction-company/{company_id}/"
        entity.add("sourceUrl", url)
        entity.add_cast("Company", "ogrnCode", row.pop("ogrn", None))

        crawl_common(context, entity, row)
        context.emit(entity, target=True)
        row.pop("logo_en", None)
        h.audit_data(row)


def crawl(context: Context) -> None:
    crawl_person(context)
    crawl_company(context)
