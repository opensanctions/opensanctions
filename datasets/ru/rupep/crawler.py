import os
import json
from typing import Any, Dict, Optional, List, Tuple
from followthemoney import model
from csv import writer
from collections import defaultdict
from normality import collapse_spaces
import re
from nomenklatura.util import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

PASSWORD = os.environ.get("OPENSANCTIONS_RUPEP_PASSWORD")
FORMATS = ["%d.%m.%Y", "%m.%Y", "%Y", "%b. %d, %Y", "%B %d, %Y"]
SPLIT_ROLES = [
    "deputy",
    "deputy head",
    "deputy chief",
    "judge",
    "deputy head",
    "member of the board",
    "member of the chamber",
    "professor",
    "associate professor",
    "deputy director",
    "mp",
    "senator",
]
REGEX_SUBNATIONAL = re.compile("(?P<area>\w{4,}) city|regional")

unknown_writer = writer(open("maybe-pep-person-roles-companies.csv", "w"))
unknown_writer.writerow(
    [
        "count",
        "role",
        "company_name",
        "id",
        "position_name",
    ]
)
unknowns = defaultdict(int)

known_writer = writer(open("known-pep-person-roles-companies.csv", "w"))
known_writer.writerow(
    [
        "count",
        "role",
        "company_name",
        "id",
        "position_name",
    ]
)
knowns = defaultdict(int)


def clean_wdid(wikidata_id: Optional[str]):
    if wikidata_id is None:
        return None
    wikidata_id = wikidata_id.strip().upper()
    if len(wikidata_id) == 0 or wikidata_id == "NONE":
        return None
    return wikidata_id


def person_id(context: Context, id: str, wikidata_id: Optional[str]):
    if is_qid(wikidata_id):
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


def crawl_person_person_relation(context: Context, entity: Entity, rel_data: dict):
    other_pep = rel_data.pop("is_pep", False)
    other_wdid = clean_wdid(rel_data.pop("person_wikidata_id"))
    other = context.make("Person")
    other.id = person_id(context, rel_data.pop("person_id"), other_wdid)
    if other.id is None:
        return
    other.add("name", rel_data.pop("person_en", None), lang="eng")
    other.add("name", rel_data.pop("person_ru", None), lang="rus")
    other.add("wikidataId", other_wdid)

    rel_type = rel_data.pop("relationship_type_en", None)
    rel_type_ru = rel_data.pop("relationship_type_ru", None)
    rel_type = rel_type or rel_type_ru
    res = context.lookup("person_person_relations", rel_type)
    if res is None:
        context.log.warn(
            "Unknown person/person relation type",
            rel_type=rel_type,
            entity=entity,
            other=other,
        )
        return

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


def crawl_company_person_relation(
    context: Context, company: Entity, person: Entity, rel_data: dict
):
    rel_type = rel_data.pop("relationship_type_en", None)
    rel_type_ru = rel_data.pop("relationship_type_ru", None)
    rel_type = rel_type or rel_type_ru
    res = context.lookup("company_person_relations", rel_type)
    if res is None:
        # context.log.info(
        #     "Unknown company/person relation type",
        #     rel_type=rel_type,
        #     entity=company,
        #     person=person_id,
        # )
        return

    if res.schema is None:
        return

    if res.schema == "Organization" and res.from_prop == "asset":
        company.schema = model.get("Company")

    rel = context.make(res.schema)
    id_a_short = short_id(context, company.id)
    id_b_short = short_id(context, person.id)
    rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
    rel.add(res.from_prop, company.id)
    rel.add(res.to_prop, person.id)
    rel.add(res.desc_prop, rel_type)
    rel.add("modifiedAt", parse_date(rel_data.pop("date_confirmed")))
    rel.add("startDate", parse_date(rel_data.pop("date_established")))
    rel.add("endDate", parse_date(rel_data.pop("date_finished")))
    context.audit_data(
        rel_data,
        ignore=[
            "is_pep",
            "person_ru",
            "person_en",
            "to_company_is_state",
            "to_company_edrpou",
            "to_company_founded",
            "to_company_key_company",
            "to_company_ru",
            "to_company_en",
            "to_company_short_ru",
            "to_company_short_en",
        ],
    )
    context.emit(rel)


def draft_position_name(role, company_name):
    if company_name.startswith("The "):
        return f"{role} of {company_name}"
    else:
        return f"{role} of the {company_name}"


def clean_position_name(role, company_name, preposition):
    if company_name.startswith("Ministry"):
        company_name = company_name.replace("Ministry of ", "")
    return f"{role} {preposition} {company_name}"


def get_subnational_area(scope, draft_position):
    if scope != "subnational":
        return None
    match = REGEX_SUBNATIONAL.match(draft_position)
    if match:
        return match.group("area")
    return None


def get_position_name(context, role, company_name, company_id) -> Optional[str]:
    if role and company_name:
        position_name = draft_position_name(role, company_name)
    else:
        # context.warning("Not handling incomplete english yet")
        return None, None

    pep_position = context.lookup("pep_positions", position_name)
    if pep_position:
        subnational_area = get_subnational_area(pep_position.scope, position_name)
        if pep_position.name:
            return pep_position.name, subnational_area
        else:
            return (
                clean_position_name(role, company_name, pep_position.preposition or "of the"),
                subnational_area,
            )

    # conext.log.warning("Unknown position", position=position_name)
    return None, None


def emit_pep_relationship(
    context: Context,
    org_id: str,
    person: Entity,
    position_name: str,
    countries: List[str],
    subnational_area: Optional[str],
    start_date: Optional[List[str]],
    end_date: Optional[List[str]],
    url: Optional[str],
    also: Optional[List[str]],
) -> None:
    position = h.make_position(
        context,
        position_name,
        country=countries,
        subnational_area=subnational_area,
    )
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=start_date,
        end_date=end_date,
    )
    if occupancy:
        occupancy.add("description", also)

        print(
            "OCCUPANCY",
            position.get("name"),
            position.get("country"),
            position.get("subnationalArea"),
            occupancy.get("status"),
            occupancy.get("startDate"),
            occupancy.get("endDate"),
        )
        context.emit(position)
        context.emit(occupancy)


def crawl_person(context: Context, companies: Dict[int, Entity], data: Dict[str, Any]):
    is_pep = data.pop("is_pep", False)
    entity = context.make("Person")
    wikidata_id = clean_wdid(data.pop("wikidata_id", None))
    rupep_person_id = data.pop("id")
    entity.id = person_id(context, rupep_person_id, wikidata_id)
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
        res = context.lookup("person_country_links", rel_type)
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
        crawl_person_person_relation(context, entity, rel_data)

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

    pep_category = person_topic.value is not None and person_topic.value in {
        "role.pep",
        "gov.igo",
    }

    for rel_data in data.pop("related_companies", []):
        company_name_ru = rel_data.get("to_company_ru", None)
        company_name_short_ru = rel_data.get("to_company_short_ru", None)
        company_name_ru = company_name_short_ru or company_name_ru
        company_name = rel_data.get("to_company_en", None)
        company_name_short = rel_data.get("to_company_short_en", None)
        company_name = company_name_short or company_name

        role = rel_data.get("relationship_type_en", None)
        role_ru = rel_data.get("relationship_type_ru", None)

        if not (role and company_name):
            # context.warn("Remember to deal with incomplete english positions")
            continue

        rupep_company_id = rel_data.pop("company_id")
        company = companies.get(rupep_company_id, None)
        if not company:
            context.log.warning(
                "Unseen company referenced in relation",
                person_id=rupep_person_id,
                company_id=rupep_company_id,
            )
            continue
        company_entity_id = company_id(context, rupep_company_id)

        start_date = parse_date(rel_data.get("date_established", None))
        end_date = parse_date(rel_data.get("date_finished", None))

        extra = None
        # If the role starts with any of the common PEPish first role parts,
        # use that and just log the rest in the occupancy
        for split in SPLIT_ROLES:
            if role.lower().startswith(split + ","):
                role, extra = role.split(",", 1)
                break

        position_name, subnational_area = get_position_name(
            context,
            collapse_spaces(role),
            collapse_spaces(company_name),
            rupep_company_id,
        )

        if position_name:
            emit_pep_relationship(
                context,
                company_entity_id,
                entity,
                position_name,
                company.get("country"),
                subnational_area,
                start_date[0] if start_date else None,
                end_date[0] if end_date else None,
                url_en,
                extra,
            )
        else:
            crawl_company_person_relation(context, company, entity, rel_data)

    data.pop("declarations", None)
    # h.audit_data(data)
    context.emit(entity, target=is_pep)


def crawl_peps(context: Context):
    companies = crawl_companies(context)

    auth = ("opensanctions", PASSWORD)
    path = context.fetch_resource("persons.json", context.data_url, auth=auth)
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        persons = json.load(fh)
    for data in persons:
        crawl_person(context, companies, data)

    # ==========================================================================
    # DEBUG
    #
    print(f"Known: {len(knowns)}  Unknown: {len(unknowns)}")

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


def crawl_company(context: Context, data: Dict[str, Any]):
    entity = context.make("Organization")
    rupep_id = data.pop("id")
    entity.id = company_id(context, rupep_id)
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
        res = context.lookup("company_country_links", rel_type)
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

    if data.pop("state_company", False):
        entity.add("topics", "gov.soe")

    ignore = [
        "wiki",
        "bank_name",
        "other_founders",
        "other_owners",
        "other_managers",
        "other_recipient",
        "related_persons",
    ]
    context.audit_data(data, ignore=ignore)
    context.emit(entity)

    return rupep_id, entity


def crawl_companies(context: Context):
    auth = ("opensanctions", PASSWORD)
    path = context.fetch_resource(
        "companies.json", "https://rupep.org/opendata/companies/json", auth=auth
    )
    # context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        companies = json.load(fh)

    company_entities = {}
    for data in companies:
        rupep_id, company = crawl_company(context, data)
        company_entities[rupep_id] = company

    return company_entities
