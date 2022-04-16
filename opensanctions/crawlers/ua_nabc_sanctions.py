import json
from urllib.parse import urljoin
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h

COUNTRIES = {
    0: "RU",
    1: "RU",
    2: "BY",
    3: "UA",
    4: None,
    None: None,
}


def parse_date(date):
    if date is not None:
        date = date.strip()
    return h.parse_date(date, ["%d.%m.%Y"])


def json_resource(context: Context, url, name):
    full_url = urljoin(url, name)
    path = context.fetch_resource(f"{name}.json", full_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        return json.load(fh)


def crawl_person(context: Context) -> None:
    data = json_resource(context, context.dataset.data.url, "person")
    for row in data["data"]:
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
        company_id = row.pop("company_id")
        name = row.pop("name")
        entity = context.make("Organization")
        entity.id = context.make_slug("company", company_id, name)
        entity.add("name", name)

        country = row.get("country", None)
        entity.add("country", COUNTRIES[country])
        entity.add("topics", "sanction")
        context.emit(entity, target=True)


def crawl(context: Context) -> None:
    crawl_person(context)
    crawl_company(context)
