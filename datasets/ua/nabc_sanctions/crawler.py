import json
from banal import ensure_list
from typing import Any, Dict, List
from urllib.parse import urljoin
from pantomime.types import JSON

from zavod import Context, Entity
from zavod import helpers as h

COUNTRIES = {
    "SCT": "GB-SCT",
    # "ATA": None,
    None: None,
}

TRACK_COUNTRIES = ["ua", "eu", "us", "au", "ca", "ch", "es", "gb", "jp", "nz", "pl"]

EXISTING_COMPANY_IDS = set()
EXISTING_PERSON_IDS = set()

# endpoints to get companies with value indicating if sanctioned
COMPANY_ENDPOINTS = {"v6/company/": True, "v6/company-warning/": False}

# endpoints to get persons with value indicating if sanctioned
PERSON_ENDPOINTS = {
    "v6/person/": True,
    "v6/person-warning/": False,
    "v6/person-relations/": False,
}


CRAWLED_PERSONS = set()  # tracks person_ids that have already been crawled
CRAWLED_COMPANIES = set()  # tracks company_ids that have already been crawled


def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
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
    for part in h.multi_split(date, [",", "\n", ";"]):
        dates.update(h.parse_date(part, ["%d.%m.%Y", "dd.%m.%Y"]))
    return dates


def url_split(urls):
    urls = []
    for url in h.multi_split(urls, [";", "\n"]):
        if len(url) > 5:
            urls.append(url)
    return urls


def iter_relations(context, data):
    if isinstance(data, list):
        if len(data) > 0:
            context.log.warning("Relations are a list", rel=data)
    elif isinstance(data, dict):
        for rel in data.values():
            yield rel
    else:
        context.log.warning("Relations are other", rel=data)


def make_person_id(id: str) -> str:
    return f"ua-nazk-person-{id}"


def make_company_id(id: str) -> str:
    return f"ua-nazk-company-{id}"


def json_listing(context: Context, url, name):
    full_url = urljoin(url, name)

    path = context.fetch_resource(f"{name}.json", full_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        resp_data = json.load(fh)
    data = resp_data["data"]
    if isinstance(data, dict):
        raise ValueError("Listing did not return an array: %s" % full_url)
    for item in data:
        yield clean_row(item)


def crawl_common(
    context: Context, entity: Entity, row: Dict[str, Any], is_sanctioned: bool
):

    country = row.pop("country", None)
    entity.add("country", COUNTRIES.get(country, country))
    keywords: List[str] = ensure_list(row.pop("category", []))
    keywords.extend(ensure_list(row.pop("subcategory_1", [])))
    keywords.extend(ensure_list(row.pop("subcategory_2", [])))
    keywords.extend(ensure_list(row.pop("subcategory_3", [])))
    for kw in keywords:
        if not kw.startswith("category_"):
            # context.log.info("KW", kw)
            entity.add("keywords", kw)

    entity.add("website", url_split(row.pop("link", "")))
    entity.add("innCode", row.pop("itn", None))
    entity.add("address", row.pop("address_ru", None), lang="rus")
    entity.add("address", row.pop("address_uk", None), lang="ukr")
    entity.add("address", row.pop("address_en", None), lang="eng")

    if is_sanctioned:
        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)
        sanction.add("startDate", row.pop("sanctions_ua_date", None))
        if row.pop("sanctions_ua", None) == 1:
            sanction.add("status", "active")

        sanction.add("sourceUrl", url_split(row.pop("url_ua", "")))

        sanction.add("reason", row.pop("reasoning_en", None), lang="eng")
        sanction.add("reason", row.pop("reasoning_ru", None), lang="rus")
        sanction.add("reason", row.pop("reasoning_uk", None), lang="ukr")
        context.emit(sanction)
    else:
        entity.add("notes", row.pop("reasoning_en", None), lang="en")
        entity.add("notes", row.pop("reasoning_ru", None), lang="rus")
        entity.add("notes", row.pop("reasoning_uk", None), lang="ukr")

    row.pop("status", None)
    row.pop("synchron", None)
    row.pop("top_50", None)
    row.pop("link_archive", None)
    row.pop("sort_order", None)

    for rel in iter_relations(context, row.pop("relations_company", [])):
        rel_name = rel.get("relation_name")
        rel_company_id = make_company_id(rel.get("company_id"))

        if int(rel.get("company_id")) not in EXISTING_COMPANY_IDS:
            context.log.warn(
                f"Skipping: company {rel_company_id} does not exist in dataset"
            )
            continue

        rel = context.lookup("relations", rel_name)
        if rel is None:
            context.log.warn(
                "Unknown relation type",
                name=rel_name,
                local=entity.id,
                remote=rel_company_id,
            )
            continue

        rel_obj = context.make(rel.schema)
        rel_obj.id = context.make_id(rel_name, entity.id, rel_company_id)
        rel_obj.add(rel.local, entity.id)
        rel_obj.add(rel.remote, rel_company_id)
        rel_obj.add("role", rel_name)
        context.emit(rel_obj)

    for rel in iter_relations(context, row.pop("relations_person", None)):
        rel_name = rel.get("relation_name")
        rel_person_id = make_person_id(rel.get("person_id"))

        if int(rel.get("person_id")) not in EXISTING_PERSON_IDS:
            context.log.warn(
                f"Skipping: person {rel_person_id} does not exist in dataset"
            )
            continue

        rel = context.lookup("relations", rel_name)
        if rel is None:
            context.log.warn(
                "Unknown relation type",
                name=rel_name,
                local=entity.id,
                remote=rel_person_id,
            )
            continue

    for cc in TRACK_COUNTRIES:
        row.pop(f"sanctions_{cc}", None)
        row.pop(f"sanctions_{cc}_date", None)
        row.pop(f"url_{cc}", None)


def crawl_person(context: Context) -> None:
    for endpoint, sanction_status in PERSON_ENDPOINTS.items():
        for row in json_listing(context, context.data_url, endpoint):
            row = clean_row(row)
            person_id = row.pop("person_id", None)
            if person_id is None:
                context.log.error("No person_id", name=row.get("name_en"))
                continue

            if int(person_id) in CRAWLED_PERSONS:
                raise RuntimeError(
                    f"Already seen person_id {person_id} in another endpoint."
                )

            entity = context.make("Person")
            entity.id = make_person_id(person_id)
            entity.add("name", row.pop("name_en", None), lang="eng")
            entity.add("name", row.pop("name_ru", None), lang="rus")
            entity.add("name", row.pop("name_uk", None), lang="ukr")
            entity.add("birthDate", parse_date(row.pop("date_bd", None)))
            entity.add("deathDate", parse_date(row.pop("date_dead", None)))
            url = f"https://sanctions.nazk.gov.ua/sanction-person/{person_id}/"
            entity.add("sourceUrl", url)
            if row.get("city_bd_en") != "N/A":
                entity.add("birthPlace", row.pop("city_bd_en", None), lang="eng")
                entity.add("birthPlace", row.pop("city_bd_ru", None), lang="rus")
                entity.add("birthPlace", row.pop("city_bd_uk", None), lang="ukr")
            entity.add("position", row.pop("position_en", None), lang="eng")
            entity.add("position", row.pop("position_ru", None), lang="rus")
            entity.add("position", row.pop("position_uk", None), lang="ukr")

            # TODO: emit image
            row.pop("photo_name", None)

            crawl_common(context, entity, row, sanction_status)
            context.emit(entity, target=sanction_status)
            context.audit_data(row)
            CRAWLED_PERSONS.add(int(person_id))


def crawl_company(context: Context) -> None:
    for endpoint, sanction_status in COMPANY_ENDPOINTS.items():
        for row in json_listing(context, context.data_url, endpoint):
            row = clean_row(row)
            company_id = row.pop("company_id")

            if int(company_id) in CRAWLED_COMPANIES:
                raise RuntimeError(
                    f"Already seen company_id {company_id} in another endpoint."
                )

            entity = context.make("Organization")
            entity.id = make_company_id(company_id)
            entity.add("name", row.pop("name_en", None), lang="eng")
            entity.add("name", row.pop("name_uk", None), lang="ukr")
            entity.add("name", row.pop("name_ru", None), lang="rus")
            entity.add("notes", row.pop("comment", None), lang="ukr")
            entity.add("innCode", row.pop("inn", None))
            url = f"https://sanctions.nazk.gov.ua/en/sanction-company/{company_id}/"
            entity.add("sourceUrl", url)
            entity.add_cast("Company", "ogrnCode", row.pop("ogrn", None))

            crawl_common(context, entity, row, sanction_status)
            context.emit(entity, target=sanction_status)
            row.pop("logo_en", None)
            ignores = [
                "my_status",
                "status_partner_db",
                "verified",
                "chk_addr",
                "chk_addr_en",
                "chk_reason",
                "logo",
            ]
            context.audit_data(row, ignore=ignores)
            CRAWLED_COMPANIES.add(int(company_id))


def get_existing_companies(context: Context):
    for endpoint in COMPANY_ENDPOINTS:
        for row in json_listing(context, context.data_url, endpoint):
            company_id = row.pop("company_id")
            EXISTING_COMPANY_IDS.add(int(company_id))


def get_existing_persons(context: Context):
    for endpoint in PERSON_ENDPOINTS:
        for row in json_listing(context, context.data_url, endpoint):
            person_id = row.pop("person_id", None)
            EXISTING_PERSON_IDS.add(int(person_id))


def crawl(context: Context) -> None:
    get_existing_companies(context)
    get_existing_persons(context)

    crawl_person(context)
    crawl_company(context)
