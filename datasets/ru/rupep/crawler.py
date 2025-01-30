import re
import ijson
from typing import Any, Dict, Optional, List, Tuple, Set

from followthemoney import model
from followthemoney.types import registry
from normality import collapse_spaces
from rigour.ids.wikidata import is_qid
import normality

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise
from zavod.runtime.lookups import type_lookup
from zavod.shed.internal_data import fetch_internal_data

# PASSWORD = os.environ.get("OPENSANCTIONS_RUPEP_PASSWORD")
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
OBVIOUSLY_NOT_PEP_ROLES = {
    "CEO",
    "owner",
    "co-owner",
    "founder",
    "co-founder",
    "employee",
    "beneficiary",
    "Senior Lecturer",
    "shareholder",
}
REGEX_SUBNATIONAL = re.compile(r"(?P<area>\w{4,}) city|regional")
PUNCTUATION = {
    "Pc": "-",
    "Pd": "-",
    "Ps": "(",
    "Pe": ")",
    "Pi": '"',
    "Pf": '"',
    "Po": "-",
}


class Company:
    """Minimal information we want to hold in memory to pass between company and
    person file passes"""

    def __init__(self, rupep_id: int, countries: Set[str]) -> None:
        self.rupep_id = rupep_id
        self.emit = False
        self.schema = None
        self.countries = countries


def clean_wdid(wikidata_id: Optional[str]):
    if wikidata_id is None:
        return None
    wikidata_id = wikidata_id.strip().upper()
    if len(wikidata_id) == 0 or wikidata_id == "NONE":
        return None
    return wikidata_id


def person_id(context: Context, id: str, wikidata_id: Optional[str]):
    if wikidata_id is not None and is_qid(wikidata_id):
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


def crawl_person_person_relation(
    context: Context, published_people: Set[int], entity: Entity, rel_data: dict
):
    other_rupep_id = rel_data.pop("person_id")
    if other_rupep_id not in published_people:
        context.log.debug("Skipping unpublished person", id=other_rupep_id)
        return
    other_pep = rel_data.pop("is_pep", False)
    other_wdid = clean_wdid(rel_data.pop("person_wikidata_id"))
    other_id = person_id(context, other_rupep_id, other_wdid)
    other = context.make("Person")
    other.id = other_id
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
    if entity.id == other_id:
        context.log.info(
            "Skipping self-relation", id=other_id, schema=res.schema, rel_type=rel_type
        )
        return

    rel = context.make(res.schema)
    id_a_short = short_id(context, entity.id)
    id_b_short = short_id(context, other.id)
    if id_a_short > id_b_short:
        # For a pair, this is true in one direction and false in the other.
        return
    rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
    rel.add(res.from_prop, entity.id)
    rel.add(res.to_prop, other.id)
    rel.add(res.desc_prop, rel_type)
    h.apply_date(rel, "modifiedAt", rel_data.pop("date_confirmed"))
    h.apply_date(rel, "startDate", rel_data.pop("date_established"))
    h.apply_date(rel, "endDate", rel_data.pop("date_finished"))

    context.audit_data(rel_data)
    context.emit(other, target=other_pep)
    context.emit(rel)


def crawl_company_person_relation(
    context: Context, company: Company, person: Entity, rel_data: dict
):
    """Also has side-effect of changing company schema for asset ownership"""
    rel_type = rel_data.pop("relationship_type_en")
    rel_type_ru = rel_data.pop("relationship_type_ru")
    rel_type = rel_type or rel_type_ru
    res = context.lookup("company_person_relations", rel_type)
    if res is None:
        # context.log.info(
        #     "Unknown company/person relation type",
        #     rel_type=rel_type,
        #     entity=company,
        #     person=person,
        # )
        return

    if res.schema is None:
        return False

    if res.schema == "Ownership" and res.from_prop == "asset":
        company.schema = model.get("Company")

    rupep_company_id = rel_data.pop("company_id")
    entity_company_id = company_id(context, rupep_company_id)
    rel = context.make(res.schema)
    id_a_short = short_id(context, entity_company_id)
    id_b_short = short_id(context, person.id)
    rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
    rel.add(res.from_prop, entity_company_id)
    rel.add(res.to_prop, person.id)
    rel.add(res.desc_prop, rel_type)
    h.apply_date(rel, "modifiedAt", rel_data.pop("date_confirmed"))
    h.apply_date(rel, "startDate", rel_data.pop("date_established"))
    h.apply_date(rel, "endDate", rel_data.pop("date_finished"))
    context.audit_data(
        rel_data,
        ignore=[
            "is_pep",
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
    return True


def draft_position_name(role, company_name):
    if company_name.lower().startswith("the "):
        return f"{role} of {company_name}"
    else:
        return f"{role} of the {company_name}"


def clean_position_name(role, company_name, preposition):
    if not preposition:
        raise ValueError("No preposition %s, %s" % (role, company_name))
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


def get_position_name(context, role, company_name) -> Optional[str]:
    if role in OBVIOUSLY_NOT_PEP_ROLES:
        return None, None, False
    if "LLC" in company_name:
        return None, None, False
    if role and company_name:
        position_name = draft_position_name(role, company_name)
    else:
        # context.warning("Not handling incomplete english yet")
        return None, None, False

    pep_position = context.lookup("pep_positions", position_name)
    if pep_position:
        subnational_area = get_subnational_area(pep_position.scope, position_name)
        if pep_position.name:
            return pep_position.name, subnational_area, True
        else:
            return (
                clean_position_name(role, company_name, pep_position.preposition),
                subnational_area,
                True,
            )
    # Skip cyrillic names in the english field for now.
    position_name = normality.category_replace(position_name, PUNCTUATION)
    if position_name == normality.latinize_text(position_name):
        return position_name, None, None
    else:
        context.log.debug(
            "Skipping probably cyrillic position name", name=position_name
        )
        return None, None, False


def emit_pep_relationship(
    context: Context,
    person: Entity,
    position_name: str,
    countries: List[str],
    subnational_area: Optional[str],
    start_date: Optional[List[str]],
    end_date: Optional[List[str]],
    also: Optional[List[str]],
    auto_pep: Optional[bool] = None,
) -> None:
    position = h.make_position(
        context,
        position_name,
        country=countries,
        subnational_area=subnational_area,
    )
    categorisation = categorise(context, position, is_pep=auto_pep)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy:
            occupancy.add("description", also)
            context.emit(position)
            context.emit(occupancy)
        return True
    else:
        return False


def crawl_person(
    context: Context,
    published_people: Set[int],
    company_state: Dict[int, Company],
    data: Dict[str, Any],
):
    data.pop("is_pep", False)
    entity = context.make("Person")
    wikidata_id = clean_wdid(data.pop("wikidata_id", None))
    rupep_person_id = data.pop("id")
    entity.id = person_id(context, rupep_person_id, wikidata_id)
    if entity.id is None:
        return
    # entity.add("sourceUrl", data.pop("url_en", None))
    data.pop("url_ru", None)
    data.pop("url_en", None)
    entity.add("modifiedAt", data.pop("last_change", None))
    entity.add("wikidataId", wikidata_id)
    entity.add("name", data.pop("full_name_en", None), lang="eng")
    entity.add("name", data.pop("full_name_ru", None), lang="rus")
    entity.add("alias", data.pop("inversed_full_name_en", None), lang="eng")
    entity.add("alias", data.pop("inversed_full_name_ru", None), lang="rus")
    entity.add("alias", data.pop("also_known_as_en", None), lang="eng")
    entity.add("alias", data.pop("also_known_as_ru", None), lang="rus")
    entity.add("alias", split_names(data.pop("names", [])))
    h.apply_date(entity, "birthDate", data.pop("date_of_birth"))
    h.apply_date(entity, "deathDate", data.pop("termination_date_human"))
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
            position = collapse_spaces(f"{org} ({role})")
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
        crawl_person_person_relation(context, published_people, entity, rel_data)

    data.pop("died", None)
    data.pop("tags", None)
    data.pop("reason_of_termination_en", None)
    data.pop("reason_of_termination_ru", None)
    # TODO: store images
    data.pop("photo", None)

    we_pep = False
    for rel_data in data.pop("related_companies", []):
        company_name_ru = rel_data.get("to_company_ru", None)
        company_name_short_ru = rel_data.get("to_company_short_ru", None)
        company_name_ru = company_name_short_ru or company_name_ru
        company_name = rel_data.get("to_company_en", None)
        company_name_short = rel_data.get("to_company_short_en", None)
        company_name = company_name_short or company_name

        role = rel_data.get("relationship_type_en", None)
        # rel_data.get("relationship_type_ru", None)

        if not (role and company_name):
            # context.warn("Remember to deal with incomplete english positions")
            continue

        rupep_company_id = rel_data.get("company_id")
        company = company_state.get(rupep_company_id, None)
        if company is None:
            context.log.debug(
                "Skipping unpublished company in person-company relation.",
                person_id=rupep_person_id,
                company_id=rupep_company_id,
            )
            continue

        start_date = rel_data.get("date_established", None)
        end_date = rel_data.get("date_finished", None)

        extra = None
        # If the role starts with any of the common PEPish first role parts,
        # use that and just log the rest in the occupancy
        for split in SPLIT_ROLES:
            if role.lower().startswith(split + ","):
                role, extra = role.split(",", 1)
                break

        # Is it automatically categorised as a PEP?
        position_name, subnational_area, auto_pep = get_position_name(
            context,
            collapse_spaces(role),
            collapse_spaces(company_name),
        )

        # Is it categorised as a PEP in the database?
        if position_name and emit_pep_relationship(
            context,
            entity,
            position_name,
            company.countries,
            subnational_area,
            start_date[0] if start_date else None,
            end_date[0] if end_date else None,
            extra,
            auto_pep,
        ):
            we_pep = True
        else:
            if crawl_company_person_relation(context, company, entity, rel_data):
                company.emit = True

    data.pop("type_of_official_ru", None)
    person_type = data.pop("type_of_official_en", None)

    res = context.lookup("person_type", person_type)
    if res is None:
        context.log.warn("Unknown type of official", type=person_type)
    if res.value:
        entity.add("topics", res.value, original_value=person_type)
    elif not we_pep:
        entity.add("topics", "poi")
    entity.add("status", person_type)

    data.pop("declarations", None)
    # h.audit_data(data)
    context.emit(entity, target=we_pep)


def get_company_country(
    context: Context, country_data: Dict
) -> Optional[Tuple[str, str, str]]:
    rel_type = country_data.pop("relationship_type")
    country_name_en = country_data.pop("to_country_en", None)
    country_name_ru = country_data.pop("to_country_ru", None)
    res = context.lookup("company_country_links", rel_type)
    if res is None:
        context.log.warn(
            "Unknown country link",
            rel_type=rel_type,
            country_name_en=country_name_en,
            country_name_ru=country_name_ru,
        )
        return None
    if res.prop is not None:
        return res.prop, country_name_en, country_name_ru
    else:
        return None


def get_company_countries(context: Context, data: Dict) -> Set[str]:
    """Clean set of countries the way we eventually will in crawl_company"""
    countries: Set[str] = set()
    for country_data in data.pop("related_countries", []):
        company_country = get_company_country(context, country_data)
        if company_country is None:
            continue
        _, name_en, name_ru = company_country
        for name in [name_en, name_ru]:
            for country in type_lookup(context.dataset, registry.country, name):
                countries.add(country)
    return countries


def crawl_company(
    context: Context, company_state: Dict[int, Company], data: Dict[str, Any]
):
    rupep_id = data.pop("id")
    company = company_state[rupep_id]
    schema = company.schema
    if schema is None:
        schema = "Organization"
    entity = context.make(schema)
    entity.id = company_id(context, rupep_id)
    # entity.add("sourceUrl", data.pop("url_en", None))
    data.pop("url_en", None)
    data.pop("url_ru", None)
    entity.add("name", data.pop("name_en", None), lang="eng")
    entity.add("name", data.pop("name_ru", None), lang="rus")
    entity.add("name", data.pop("name_suggest_output_ru", None), lang="rus")
    entity.add("name", data.pop("name_suggest_output_en", None), lang="eng")
    entity.add("alias", data.pop("also_known_as", None))
    entity.add("alias", data.pop("short_name_en", None), lang="eng")
    entity.add("alias", data.pop("short_name_ru", None), lang="rus")
    h.apply_date(entity, "incorporationDate", data.pop("founded"))
    h.apply_date(entity, "dissolutionDate", data.pop("closed"))
    entity.add("status", data.pop("status_en", data.pop("status_ru", None)))
    entity.add("status", data.pop("status", None))
    entity.add_cast("Company", "ogrnCode", data.pop("ogrn_code", None))
    entity.add("registrationNumber", data.pop("edrpou", None))

    for country_data in data.pop("related_countries", []):
        company_country = get_company_country(context, country_data)
        if company_country is not None:
            prop, country_name_en, country_name_ru = company_country
            entity.add(prop, country_name_ru, lang="rus")
            entity.add(prop, country_name_en, lang="eng")

    related_companies = data.pop("related_companies", [])
    for rel_data in related_companies:
        other_rupep_id = rel_data.pop("company_id")
        if other_rupep_id not in company_state:
            context.log.debug(
                "Skipping unpublished company in company-company relation",
                id=other_rupep_id,
            )
            continue
        other_id = company_id(context, other_rupep_id)

        rel_type = rel_data.pop("relationship_type_en", None)
        rel_type_ru = rel_data.pop("relationship_type_ru", None)
        rel_type = rel_type or rel_type_ru

        res = context.lookup("company_company_relations", rel_type)
        if res is None:
            context.log.warn(
                "Unknown company/company relation type",
                rel_type=rel_type,
                entity=entity,
                other=other_id,
            )
            continue

        if res.schema is None:
            continue

        if entity.id == other_id:
            context.log.info(
                "Skipping self-relation",
                id=other_id,
                schema=res.schema,
                rel_type=rel_type,
            )
            continue

        # if res.schema == "Organization" and res.from_prop == "asset":
        #     entity.schema = model.get("Company")
        rel = context.make(res.schema)
        id_a_short = short_id(context, entity.id)
        id_b_short = short_id(context, other_id)
        if id_a_short > id_b_short:
            # For a pair, this is true in one direction and false in the other.
            continue
        rel.id = context.make_slug(id_a_short, res.schema, id_b_short)
        rel.add(res.from_prop, entity.id)
        rel.add(res.to_prop, other_id)
        rel.add(res.desc_prop, rel_type)
        h.apply_date(rel, "modifiedAt", rel_data.pop("date_confirmed"))
        h.apply_date(rel, "startDate", rel_data.pop("date_established"))
        h.apply_date(rel, "endDate", rel_data.pop("date_finished"))
        context.emit(rel)
        context.audit_data(
            rel_data,
            ignore=["state_company", "key_company", "company_ru", "company_en"],
        )

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


def crawl(context: Context):
    # auth = ("opensanctions", PASSWORD)
    companies_path = context.get_resource_path("companies.json")
    if not companies_path.exists():
        fetch_internal_data("rupep/20240118/companies.json", companies_path)
    persons_path = context.get_resource_path("persons.json")
    if not persons_path.exists():
        fetch_internal_data("rupep/20240118/persons.json", persons_path)
    # Only emit companies and people who occur in the root array in the source data.
    # That's how RuPEP indicates that they are published and available for publication
    # in OpenSanctions.

    published_people = set()
    context.log.info("Loading published person IDs.")
    with open(persons_path, "r") as fh:
        for data in ijson.items(fh, "item"):
            published_people.add(data.get("id"))

    # Build a dict of Company instances to pass company countries to Position
    # extraction, and pass the decision to emit a company from crawl_person
    # back to crawl_company.
    company_state: Dict[int, Company] = {}
    context.log.info("Loading initial company state.")
    with open(companies_path, "r") as fh:
        for data in ijson.items(fh, "item"):
            rupep_company_id = data.get("id")
            countries = get_company_countries(context, data)
            company = Company(rupep_company_id, countries)
            if data.get("ogrn_code", None):
                company.emit = True
            company_state[company.rupep_id] = company

    context.log.info("Creating persons and positions.")
    with open(persons_path, "r") as fh:
        for data in ijson.items(fh, "item"):
            crawl_person(context, published_people, company_state, data)

    # This is only really needed if we care enough about excluding companies
    # that aren't linked to any people or other emitted companies to tolerate
    # this compute and ugliness cost.
    changed = True
    propagations = 0
    max_propagations = 20
    while changed:
        if propagations >= max_propagations:
            context.log.warning("Maxed out propagations. Not propagating further.")
            break
        changed = False
        propagations += 1
        context.log.info(
            f"Propagating emit decision along company relations (it {propagations})."
        )
        with open(companies_path, "r") as fh:
            for data in ijson.items(fh, "item"):
                company = company_state.get(data.get("id"))
                if company.emit:
                    for rel_data in data.get("related_companies", []):
                        other_rupep_id = rel_data.get("company_id")
                        other_company = company_state.get(other_rupep_id, None)
                        if other_company is None:
                            context.log.debug(
                                "Skipping unpublished company in nested company-company relation",
                                company_id=other_rupep_id,
                            )
                            continue
                        if not other_company.emit:
                            changed = True
                            other_company.emit = True

    context.log.info("Creating companies.")
    with open(companies_path, "r") as fh:
        for data in ijson.items(fh, "item"):
            company = company_state[data.get("id")]
            if company.emit:
                crawl_company(context, company_state, data)
