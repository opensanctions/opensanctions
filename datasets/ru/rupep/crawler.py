import os
import json
from typing import Any, Dict, Optional, List
from followthemoney import model
from csv import writer
from collections import defaultdict

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

PASSWORD = os.environ.get("OPENSANCTIONS_RUPEP_PASSWORD")
FORMATS = ["%d.%m.%Y", "%m.%Y", "%Y", "%b. %d, %Y", "%B %d, %Y"]

unknown_writer = writer(open("maybe-pep-person-roles-companies.csv", "w"))
unknown_writer.writerow(["count", "role", "role_ru", "name", "name_ru", "is_state", "id"])
unknowns = defaultdict(int)

known_writer = writer(open("known-pep-person-roles-companies.csv", "w"))
known_writer.writerow(["count", "role", "role_ru", "name", "name_ru", "scope", "id"])
knowns = defaultdict(int)


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
    return h.multi_split(names, ["\n", ", "])


def parse_date(date):
    if date is None or len(date.strip()) == 0:
        return None
    if h.check_no_year(date):
        return None
    date = date.replace("Sept.", "Sep.")
    return h.parse_date(date, FORMATS)


def crawl_person(context: Context, data: Dict[str, Any]):
    is_pep = data.pop("is_pep", False)
    entity = context.make("Person")
    wikidata_id = clean_wdid(data.pop("wikidata_id", None))
    entity.id = person_id(context, data.pop("id"), wikidata_id)
    if entity.id is None:
        return
    url_en = data.pop("url_en", None)
    entity.add("sourceUrl", url_en)
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

    last_positions = set()

    for lang, suffix in ((None, ""), ("eng", "_en"), ("rus", "_ru")):
        role = data.pop(f"last_job_title{suffix}", None)
        org = data.pop(f"last_workplace{suffix}", None)
        if org is None or not len(org.strip()):
            continue
        last_positions.add(org)
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
        ignores = [
            "country_code",
            "date_established",
            "date_confirmed",
            "date_finished",
        ]
        context.audit_data(country_data, ignore=ignores)

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

        context.audit_data(rel_data)
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


    if person_topic.value is not None and ("role.pep" in person_topic.value or "gov.igo" in person_topic.value):
        has_state_company = False
        for company_data in data.pop("related_companies", []):
            name_ru = company_data.get("to_company_ru", None)
            name_short_ru = company_data.get("to_company_short_ru", None)
            name_ru = name_short_ru or name_ru
            name = company_data.get("to_company_en", None)
            name_short = company_data.get("to_company_short_en", None)
            name = name_short or name

            role = company_data.get("relationship_type_en", None)
            role_ru = company_data.get("relationship_type_ru", None)
            
            is_state = company_data.get("to_company_is_state", None)
            if is_state:
                has_state_company = True
            company_id = company_data.get("company_id")
            end_date = parse_date(company_data.get("date_finished", None))
            
            if end_date is None or end_date[0] > "2019":

                co_rel_res = context.lookup("company_relations", role)
                pep_co_res = context.lookup("pep_organizations", name)

                if co_rel_res and co_rel_res.schema != "Occupancy":
                    continue
                if co_rel_res and co_rel_res.schema == "Occupancy" and pep_co_res:
                    # "role", "role_ru", "name", "name_ru", "scope", "id"
                    key = (role, role_ru, name, name_ru, pep_co_res.scope, company_id)
                    knowns[key] += 1
                else:
                    # "role", "role_ru", "name", "name_ru", "is_state", "id"
                    key = (role, role_ru, name, name_ru, is_state, company_id)
                    unknowns[key] += 1
        if not has_state_company:
            print(f"No state company for PEP {url_en}")






    data.pop("declarations", None)
    # h.audit_data(data)
    context.emit(entity, target=is_pep)


def crawl_peps(context: Context):
    auth = ("opensanctions", PASSWORD)
    path = context.fetch_resource("persons.json", context.data_url, auth=auth)
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        persons = json.load(fh)
    for data in persons:
        crawl_person(context, data)

    sorted_knowns = []
    for key, count in knowns.items():
        sorted_knowns.append([count] + list(key))
    sorted_knowns.sort()
    sorted_knowns.reverse()
    for row in sorted_knowns:
        known_writer.writerow(row)

    sorted_unknowns = []
    for key, count in unknowns.items():
        sorted_unknowns.append([count] + list(key))
    sorted_unknowns.sort()
    sorted_unknowns.reverse()
    for row in sorted_unknowns:
        unknown_writer.writerow(row)

def emit_pep_relationship(
    context: Context,
    org: Entity,
    person_id: str,
    role_en: str,
    role_ru: str,
    org_name_en: Optional[str],
    org_name_ru: Optional[str],
    country_names_ru: List[str],
    country_names_en: List[str],
    start_date: Optional[List[str]],
    end_date: Optional[List[str]],
    url: Optional[str],
) -> None:
    if role_en and org_name_en:
        if org_name_en.startswith("The "):
            position_name = f"{role_en} of {org_name_en}"
        else:
            position_name = f"{role_en} of the {org_name_en}"
    elif role_ru and org_name_ru:
        context.log.warning(
            "Don't know how to construct russian position name",
            role=role_ru,
            org=org_name_ru,
        )
        return
    else:
        context.log.warning(
            "No common language pair to construct position name",
            role_en=role_en,
            role_ru=role_ro,
            org_en=org_name_en,
            org_ru=org_name_ru,
        )
        return
    position = h.make_position(
        context,
        position_name,
        country=country_names_en + country_names_ru,
        organization=org,
        source_url=url,
    )
    occupancy = h.make_occupancy(
        context,
        person_id,
        position,
        no_end_implies_current=True,
        start_date=start_date,
        end_date=end_date,
    )
    if occupancy:
        # print("OCCUPANCY", position.get("name"), position.get("country"), occupancy.get("status"))
        context.emit(position)
        context.emit(occupancy)


def crawl_company(context: Context, data: Dict[str, Any]):
    entity = context.make("Organization")
    entity.id = company_id(context, data.pop("id"))
    url = data.pop("url_en", None)
    entity.add("sourceUrl", url)
    data.pop("url_ru", None)
    name_en = data.pop("name_en", None)
    entity.add("name", name_en, lang="eng")
    name_ru = data.pop("name_ru", None)
    entity.add("name", name_ru, lang="rus")
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

    country_names_en = []
    country_names_ru = []

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
            country_names_ru.append(country_name_ru)
            country_names_en.append(country_name_en)
        # h.audit_data(country_data)

    is_state_company = data.pop("state_company", False)

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
            # context.log.info(
            #     "Unknown company/person relation type",
            #     rel_type=rel_type,
            #     entity=entity,
            #     other=other_id,
            # )
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
        start_date = parse_date(rel_data.pop("date_established"))
        rel.add("startDate", start_date)
        end_date = parse_date(rel_data.pop("date_finished"))
        rel.add("endDate", end_date)

        is_pep = rel_data.pop("is_pep", False)
        if is_state_company and is_pep:
            emit_pep_relationship(
                context,
                entity,
                id_a_short,
                rel_type,
                rel_type_ru,
                name_en,
                name_ru,
                country_names_ru,
                country_names_en,
                start_date,
                end_date,
                url,
            )

        context.audit_data(rel_data, ignore=["person_ru", "person_en"])
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

    address = h.format_address(
        street=data.pop("street", None),
        city=data.pop("city", None),
    )
    entity.add("address", address)

    if is_state_company:
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
    path = context.fetch_resource("companies.json", context.data_url, auth=auth)
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        companies = json.load(fh)
    for data in companies:
        crawl_company(context, data)
