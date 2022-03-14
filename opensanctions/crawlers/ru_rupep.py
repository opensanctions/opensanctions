import json
import requests
from lxml import html
from typing import Any, Dict, Optional
from contextlib import closing
from codecs import iterdecode
from datapatch.result import Result
from followthemoney.types import registry
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split


def clean_wdid(wikidata_id: Optional[str]):
    if wikidata_id is None:
        return None
    wikidata_id = wikidata_id.strip().upper()
    if len(wikidata_id) == 0 or wikidata_id != "NONE":
        return None
    return wikidata_id


def person_id(context: Context, id: str, wikidata_id: Optional[str]):
    if wikidata_id is not None:
        return wikidata_id
    return context.make_slug("person", id)


# def company_id(context: Context, id: str):
#     return context.make_slug("company", id)


def split_names(names):
    return multi_split(names, ["\n", ", "])


def parse_date(date):
    return h.parse_date(date, ["%d.%m.%Y", "%m.%Y", "%Y"])


def fetch(context: Context):
    companies_url = "https://rupep.org/opendata/companies/json"
    path = context.fetch_resource("companies.json", companies_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    path = context.fetch_resource("persons.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        return json.load(fh)


def crawl_person(context: Context, data: Dict[str, Any]):
    is_pep = data.pop("is_pep", False)
    entity = context.make("Person", target=is_pep)
    wikidata_id = clean_wdid(data.pop("wikidata_id", None))
    entity.id = person_id(context, data.pop("id"), wikidata_id)
    entity.add("sourceUrl", data.pop("url_en", None))
    data.pop("url_ru", None)
    entity.add("modifiedAt", data.pop("last_change", None))
    entity.add("wikidataId", wikidata_id)
    entity.add("name", data.pop("full_name_en", None))
    entity.add("name", data.pop("full_name_ru", None))
    entity.add("alias", data.pop("inversed_full_name_en", None))
    entity.add("alias", data.pop("inversed_full_name_ru", None))
    entity.add("alias", data.pop("also_known_as_en", None))
    entity.add("alias", data.pop("also_known_as_ru", None))
    entity.add("alias", split_names(data.pop("names", [])))
    entity.add("birthDate", parse_date(data.pop("date_of_birth", None)))
    entity.add("deathDate", parse_date(data.pop("termination_date_human", None)))
    entity.add("birthPlace", data.pop("city_of_birth_ru", None))
    entity.add("birthPlace", data.pop("city_of_birth_en", None))
    entity.add("innCode", data.pop("inn", None))
    entity.add("firstName", data.pop("first_name_en", None))
    entity.add("firstName", data.pop("first_name_ru", None))
    entity.add("fatherName", data.pop("patronymic_en", None))
    entity.add("fatherName", data.pop("patronymic_ru", None))
    entity.add("lastName", data.pop("last_name_en", None))
    entity.add("lastName", data.pop("last_name_ru", None))

    for suffix in ("", "_en", "_ru"):
        role = data.pop(f"last_job_title{suffix}", None)
        org = data.pop(f"last_workplace{suffix}", None)
        entity.add("position", f"{org} ({role})")

    for country_data in data.pop("related_countries", []):
        rel_type = country_data.pop("relationship_type")
        country_name = country_data.pop("to_country_en")
        country_name = country_name or country_data.pop("to_country_ru")
        # print(country_name)
        res = context.lookup("country_links", rel_type)
        if res is None:
            context.log.warn(
                "Unknown country link",
                rel_type=rel_type,
                entity=entity,
                country=country_name,
            )
            continue
        if res.prop is not None:
            entity.add(res.prop, country_name)
        # h.audit_data(country_data)

    for rel_data in data.pop("related_persons", []):
        other_pep = rel_data.pop("is_pep", False)
        other_wdid = clean_wdid(rel_data.pop("person_wikidata_id"))
        other = context.make("Person", target=other_pep)
        other.id = person_id(context, rel_data.pop("person_id"), other_wdid)
        other.add("name", rel_data.pop("person_en", None))
        other.add("name", rel_data.pop("person_ru", None))
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
        id_a_short = id_a.replace(context.make_slug("person"), "p")
        id_b_short = id_b.replace(context.make_slug("person"), "p")
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
    person_topic = context.lookup_value("person_type", person_type)
    if person_topic is None:
        context.log.warn("Unknown type of official", type=person_type)
    entity.add("topics", person_topic)
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


def crawl(context: Context):
    persons = fetch(context)
    for data in persons:
        crawl_person(context, data)
