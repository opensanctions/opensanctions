import os
import json
from typing import Any, Dict, Optional
from followthemoney import model
from zavod.parse import format_address

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

PASSWORD = os.environ.get("OPENSANCTIONS_RUPEP_PASSWORD")
FORMATS = ["%d.%m.%Y", "%m.%Y", "%Y", "%b. %d, %Y", "%B %d, %Y"]


def clean_wdid(wikidata_id: Optional[str]):
    if wikidata_id is None:
        return None
    wikidata_id = wikidata_id.strip().upper()
    if len(wikidata_id) == 0 or wikidata_id == "NONE":
        return None
    return wikidata_id


def person_id(context: Context, id: str, wikidata_id: Optional[str]):
    if wikidata_id is not None:
        return wikidata_id
    # Sergei Glinka, information in RuPEP doesn't properly reflect he's
    # left some business relationships.
    if id == 8095:
        return None
    return context.make_slug("person", id)


def company_id(context: Context, id: str):
    return context.make_slug("company", id)


def short_id(context: Context, id: str) -> str:
    id = id.replace(context.make_slug("person"), "p")
    id = id.replace(context.make_slug("company"), "c")
    return id


def split_names(names):
    return multi_split(names, ["\n", ", "])


def parse_date(date):
    if h.check_no_year(date):
        return None
    if date is not None:
        date = date.replace("Sept.", "Sep.")
    return h.parse_date(date, FORMATS)


def crawl_person(context: Context, data: Dict[str, Any]):
    is_pep = data.pop("is_pep", False)
    entity = context.make("Person")
    wikidata_id = clean_wdid(data.pop("wikidata_id", None))
    entity.id = person_id(context, data.pop("id"), wikidata_id)
    if entity.id is None:
        return
    entity.add("sourceUrl", data.pop("url_en", None))
    data.pop("url_ru", None)
    entity.add("modifiedAt", data.pop("last_change", None))
    entity.add("wikidataId", wikidata_id)
    entity.add("name", data.pop("full_name_en", None), lang="eng")
    entity.add("name", data.pop("full_name_ru", None), lang="rus")
    entity.add("alias", data.pop("inversed_full_name_en", None), lang="eng")
    entity.add("alias", data.pop("inversed_full_name_ru", None), lang="rus")
    entity.add("alias", data.pop("also_known_as_en", None), lang="eng")
    entity.add("alias", data.pop("also_known_as_ru", None), lang="rus")
    entity.add("alias", split_names(data.pop("names", [])))
    entity.add("birthDate", parse_date(data.pop("date_of_birth", None)))
    entity.add("deathDate", parse_date(data.pop("termination_date_human", None)))
    entity.add("birthPlace", data.pop("city_of_birth_ru", None), lang="rus")
    entity.add("birthPlace", data.pop("city_of_birth_en", None), lang="eng")
    entity.add("innCode", data.pop("inn", None))
    entity.add("firstName", data.pop("first_name_en", None), lang="eng")
    entity.add("firstName", data.pop("first_name_ru", None), lang="rus")
    entity.add("fatherName", data.pop("patronymic_en", None), lang="eng")
    entity.add("fatherName", data.pop("patronymic_ru", None), lang="rus")
    entity.add("lastName", data.pop("last_name_en", None), lang="eng")
    entity.add("lastName", data.pop("last_name_ru", None), lang="rus")

    for lang, suffix in ((None, ""), ("eng", "_en"), ("rus", "_ru")):
        role = data.pop(f"last_job_title{suffix}", None)
        org = data.pop(f"last_workplace{suffix}", None)
        if org is None or not len(org.strip()):
            continue
        position = org
        if role is not None and len(role.strip()):
            position = f"{org} ({role})"
        entity.add("position", position, lang=lang)

    for country_data in data.pop("related_countries", []):
        rel_type = country_data.pop("relationship_type")
        country_name_en = country_data.pop("to_country_en", None)
        country_name_ru = country_data.pop("to_country_ru", None)
        # print(country_name)
        res = context.lookup("country_links", rel_type)
        if res is None:
            context.log.warn(
                "Unknown country link",
                rel_type=rel_type,
                entity=entity,
                country_name_en=country_name_en,
                country_name_ru=country_name_ru,
            )
            continue
        if res.prop is not None:
            entity.add(res.prop, country_name_ru, lang="rus")
            entity.add(res.prop, country_name_en, lang="eng")
        # h.audit_data(country_data)

    for rel_data in data.pop("related_persons", []):
        other_pep = rel_data.pop("is_pep", False)
        other_wdid = clean_wdid(rel_data.pop("person_wikidata_id"))
        other = context.make("Person")
        other.id = person_id(context, rel_data.pop("person_id"), other_wdid)
        if other.id is None:
            continue
        other.add("name", rel_data.pop("person_en", None), lang="eng")
        other.add("name", rel_data.pop("person_ru", None), lang="rus")
        other.add("wikidataId", other_wdid)

        rel_type = rel_data.pop("relationship_type_en", None)
        rel_type_ru = rel_data.pop("relationship_type_ru", None)
        rel_type = rel_type or rel_type_ru
        res = context.lookup("person_relations", rel_type)
        if res is None:
            context.log.warn(
                "Unknown person/person relation type",
                rel_type=rel_type,
                entity=entity,
                other=other,
            )
            continue

        # print("LINK", (entity.id, other.id))
        id_a, id_b = sorted((entity.id, other.id))
        rel = context.make(res.schema)
        id_a_short = short_id(context, id_a)
        id_b_short = short_id(context, id_b)
        rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
        rel.add(res.from_prop, id_a)
        rel.add(res.to_prop, id_b)
        rel.add(res.desc_prop, rel_type)
        rel.add("modifiedAt", parse_date(rel_data.pop("date_confirmed")))
        rel.add("startDate", parse_date(rel_data.pop("date_established")))
        rel.add("endDate", parse_date(rel_data.pop("date_finished")))

        # h.audit_data(rel_data)
        context.emit(other, target=other_pep)
        context.emit(rel)

    data.pop("type_of_official_ru", None)
    person_type = data.pop("type_of_official_en", None)
    person_topic = context.lookup("person_type", person_type)
    if person_topic is None:
        context.log.warn("Unknown type of official", type=person_type)
    else:
        entity.add("topics", person_topic.value, original_value=person_type)
    if is_pep:
        entity.add("topics", "role.pep")
    entity.add("status", person_type)

    data.pop("died", None)
    data.pop("tags", None)
    data.pop("reason_of_termination_en", None)
    data.pop("reason_of_termination_ru", None)
    # TODO: store images
    data.pop("photo", None)
    data.pop("related_companies", None)
    data.pop("declarations", None)
    # h.audit_data(data)
    context.emit(entity, target=is_pep)


def crawl_peps(context: Context):
    auth = ("opensanctions", PASSWORD)
    path = context.fetch_resource("persons.json", context.source.data.url, auth=auth)
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        persons = json.load(fh)
    for data in persons:
        crawl_person(context, data)


def crawl_company(context: Context, data: Dict[str, Any]):
    entity = context.make("Organization")
    entity.id = company_id(context, data.pop("id"))
    entity.add("sourceUrl", data.pop("url_en", None))
    data.pop("url_ru", None)
    entity.add("name", data.pop("name_en", None), lang="eng")
    entity.add("name", data.pop("name_ru", None), lang="rus")
    entity.add("name", data.pop("name_suggest_output_ru", None), lang="rus")
    entity.add("name", data.pop("name_suggest_output_en", None), lang="eng")
    entity.add("alias", data.pop("also_known_as", None))
    entity.add("alias", data.pop("short_name_en", None), lang="eng")
    entity.add("alias", data.pop("short_name_ru", None), lang="rus")
    entity.add("incorporationDate", parse_date(data.pop("founded", None)))
    entity.add("dissolutionDate", parse_date(data.pop("closed", None)))
    entity.add("status", data.pop("status_en", data.pop("status_ru", None)))
    entity.add("status", data.pop("status", None))
    entity.add_cast("Company", "ogrnCode", data.pop("ogrn_code", None))
    entity.add("registrationNumber", data.pop("edrpou", None))

    for country_data in data.pop("related_countries", []):
        rel_type = country_data.pop("relationship_type")
        country_name_en = country_data.pop("to_country_en", None)
        country_name_ru = country_data.pop("to_country_ru", None)
        res = context.lookup("country_links", rel_type)
        if res is None:
            context.log.warn(
                "Unknown country link",
                rel_type=rel_type,
                entity=entity,
                country_name_en=country_name_en,
                country_name_ru=country_name_ru,
            )
            continue
        if res.prop is not None:
            entity.add(res.prop, country_name_ru, lang="rus")
            entity.add(res.prop, country_name_en, lang="eng")
        # h.audit_data(country_data)

    for rel_data in data.pop("related_persons", []):
        other_wdid = clean_wdid(rel_data.pop("person_wikidata_id"))
        other_id = person_id(context, rel_data.pop("person_id"), other_wdid)
        if other_id is None:
            continue

        rel_type = rel_data.pop("relationship_type_en", None)
        rel_type_ru = rel_data.pop("relationship_type_ru", None)
        rel_type = rel_type or rel_type_ru
        res = context.lookup("person_relations", rel_type)
        if res is None:
            context.log.info(
                "Unknown company/person relation type",
                rel_type=rel_type,
                entity=entity,
                other=other_id,
            )
            continue

        if res.schema is None:
            continue

        if res.schema == "Organization" and res.from_prop == "asset":
            entity.schema = model.get("Company")

        rel = context.make(res.schema)
        id_a_short = short_id(context, entity.id)
        id_b_short = short_id(context, other_id)
        rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
        rel.add(res.from_prop, entity.id)
        rel.add(res.to_prop, other_id)
        rel.add(res.desc_prop, rel_type)
        rel.add("modifiedAt", parse_date(rel_data.pop("date_confirmed")))
        rel.add("startDate", parse_date(rel_data.pop("date_established")))
        rel.add("endDate", parse_date(rel_data.pop("date_finished")))
        context.emit(rel)

    for rel_data in data.pop("related_companies", []):
        # pprint(rel_data)
        # other_id = company_id(context, rel_data.pop("company_id"))

        # rel_type = rel_data.pop("relationship_type_en", None)
        # rel_type_ru = rel_data.pop("relationship_type_ru", None)
        # rel_type = rel_type or rel_type_ru
        # res = context.lookup("company_relations", rel_type)
        # if res is None:
        #     context.log.warn(
        #         "Unknown company/company relation type",
        #         rel_type=rel_type,
        #         entity=entity,
        #         other=other_id,
        #     )
        #     continue

        # if res.schema is None:
        #     continue

        # if res.schema == "Organization" and res.from_prop == "asset":
        #     entity.schema = model.get("Company")

        # rel = context.make(res.schema)
        # id_a_short = short_id(context, entity.id)
        # id_b_short = short_id(context, other_id)
        # rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
        # rel.add(res.from_prop, entity.id)
        # rel.add(res.to_prop, other_id)
        # rel.add(res.desc_prop, rel_type)
        # rel.add("modifiedAt", parse_date(rel_data.pop("date_confirmed")))
        # rel.add("startDate", parse_date(rel_data.pop("date_established")))
        # rel.add("endDate", parse_date(rel_data.pop("date_finished")))
        # context.emit(rel)
        # h.audit_data(rel_data)
        pass

    address = format_address(
        street=data.pop("street", None),
        city=data.pop("city", None),
    )
    entity.add("address", address)

    if data.pop("state_company", False):
        entity.add("topics", "gov.soe")

    ignore = [
        "wiki",
        "bank_name",
        "other_founders",
        "other_owners",
        "other_managers",
        "other_recipient",
    ]
    context.audit_data(data, ignore=ignore)
    # print(entity.to_dict())
    context.emit(entity)


def crawl_companies(context: Context):
    auth = ("opensanctions", PASSWORD)
    path = context.fetch_resource("companies.json", context.source.data.url, auth=auth)
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        companies = json.load(fh)
    for data in companies:
        crawl_company(context, data)
