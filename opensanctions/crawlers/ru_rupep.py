import csv
import requests
from lxml import html
from typing import Dict
from contextlib import closing
from codecs import iterdecode
from datapatch.result import Result
from followthemoney.types import registry

from opensanctions.core import Context
from opensanctions import helpers as h

TYPE_OF_OFFICIAL = {
    "": "",
    "1": "Національний публічний діяч",
    "2": "Іноземний публічний діяч",
    "3": "Діяч, що виконуює значні функції в міжнародній організації",
    "4": "Пов'язана особа",
    "5": "Член сім'ї",
}


def stream_csv(table_name):
    url = "https://data.opensanctions.org/contrib/rupep/20220224/%s.csv" % table_name
    with closing(requests.get(url, stream=True)) as r:
        lines = iterdecode(r.iter_lines(), "utf-8")
        reader = csv.DictReader(lines, delimiter=",", quotechar='"')
        for row in reader:
            yield row


def strip_html(snippet):
    if snippet is None or not len(snippet.strip()):
        return None
    doc = html.fromstring(snippet)
    return doc.text_content()


def get_countries(context: Context):
    countries: Dict[str, str] = {}
    for row in stream_csv("core_country"):
        country_id = row.get("id")
        country_code = registry.country.clean_text(row.get("name_en"))
        if country_code is None:
            country_code = registry.country.clean_text(row.get("iso2"))
        if country_code is None:
            country_code = registry.country.clean_text(row.get("name_ru"))
        if row.get("iso2") == "DR":
            country_code = "dd"
        if row.get("iso2") == "AB":
            country_code = "ge-ab"
        # if country_code is None:
        #     context.pprint(row)
        if country_code is not None:
            countries[country_id] = country_code
    return countries


def person_id(context: Context, id: str):
    return context.make_slug("person", id)


def company_id(context: Context, id: str):
    return context.make_slug("company", id)


def crawl_people(context: Context, countries):
    for row in stream_csv("core_person2country"):
        entity = context.make("Person")
        entity.id = person_id(context, row.pop("from_person_id"))
        type_ = row.pop("relationship_type")
        type_res = context.lookup("country_links", type_)
        if type_res is None:
            context.log.warning("Unknown country/person relation", type_=type_)
            continue
        if type_res.prop is None:
            continue
        country_code = countries.get(row.pop("to_country_id"))
        entity.add(type_res.prop, country_code)
        context.emit(entity)
        # h.audit_data(row)

    for row in stream_csv("core_person2person"):
        from_type = row.pop("from_relationship_type")
        to_type = row.pop("to_relationship_type")
        type_ = "%s / %s" % (from_type, to_type)
        type_res = context.lookup("person_links", type_)
        if type_res is None:
            context.log.warning("Unknown person/person relation", type_=type_)
            continue
        from_person_id = person_id(context, row.pop("from_person_id"))
        to_person_id = person_id(context, row.pop("to_person_id"))
        entity = context.make(type_res.schema)
        entity.id = context.make_slug("p2p", row.pop("id"))
        entity.add(type_res.from_prop, from_person_id)
        entity.add(type_res.to_prop, to_person_id)
        entity.add(type_res.desc_prop, type_)
        entity.add("summary", strip_html(row.pop("relationship_details")))
        entity.add("summary", strip_html(row.pop("relationship_details_ru")))
        entity.add("startDate", row.pop("date_established"))
        entity.add("endDate", row.pop("date_finished"))
        context.emit(entity)
        # h.audit_data(row)

    for row in stream_csv("core_person"):
        entity = context.make("Person")
        entity.id = person_id(context, row.pop("id"))
        target = row.pop("is_pep") == "TRUE"
        if target:
            entity.add("topics", "role.pep")
        entity.add("birthDate", row.pop("dob"))
        entity.add("birthPlace", row.pop("city_of_birth"))
        entity.add("birthPlace", row.pop("city_of_birth_ru"))
        entity.add("modifiedAt", row.pop("last_change"))
        entity.add("innCode", row.pop("inn"))

        h.apply_name(
            entity,
            first_name=row.pop("first_name"),
            patronymic=row.pop("patronymic"),
            last_name=row.pop("last_name"),
        )
        h.apply_name(
            entity,
            first_name=row.pop("first_name_en"),
            patronymic=row.pop("patronymic_en"),
            last_name=row.pop("last_name_en"),
        )
        h.apply_name(
            entity,
            first_name=row.pop("first_name_ru"),
            patronymic=row.pop("patronymic_ru"),
            last_name=row.pop("last_name_ru"),
        )
        official_type = TYPE_OF_OFFICIAL[row.pop("type_of_official")]
        entity.add("position", official_type)

        # TODO: can these be split somehow?
        entity.add("weakAlias", row.pop("names"))

        context.emit(entity, target=target)
        # h.audit_data(row)


def crawl_companies(context: Context, countries):
    for row in stream_csv("core_company2country"):
        entity = context.make("Organization")
        entity.id = company_id(context, row.pop("from_company_id"))
        type_ = row.pop("relationship_type")
        type_res = context.lookup("country_links", type_)
        if type_res is None:
            context.log.warning("Unknown country/company relation", type_=type_)
            continue
        if type_res.prop is None:
            continue
        country_code = countries.get(row.pop("to_country_id"))
        entity.add(type_res.prop, country_code)
        context.emit(entity)
        # h.audit_data(row)

    for row in stream_csv("core_person2company"):
        type_ = row.pop("relationship_type_en")
        type_ = type_ or row.pop("relationship_type")
        type_res = context.lookup("company_person_links", type_)
        if type_res is None:
            # context.log.warning("Unknown person/company relation", type_=type_)
            type_res = Result(
                dict(
                    schema="UnknownLink",
                    from_prop="subject",
                    to_prop="object",
                    desc_prop="role",
                )
            )
        from_person_id = person_id(context, row.pop("from_person_id"))
        to_company_id = company_id(context, row.pop("to_company_id"))
        entity = context.make(type_res.schema)
        entity.id = context.make_slug("p2c", row.pop("id"))
        entity.add(type_res.from_prop, from_person_id)
        entity.add(type_res.to_prop, to_company_id)
        entity.add(type_res.desc_prop, type_)
        entity.add("startDate", row.pop("date_established"))
        entity.add("endDate", row.pop("date_finished"))
        context.emit(entity)
        # h.audit_data(row)

    for row in stream_csv("core_company2company"):
        type_ = row.pop("relationship_type")
        type_res = context.lookup("company_links", type_)
        if type_res is None:
            context.log.warning("Unknown company/company relation", type_=type_)
            # type_res = Result(
            #     dict(
            #         schema="UnknownLink",
            #         from_prop="subject",
            #         to_prop="object",
            #         desc_prop="role",
            #     )
            # )
            continue
        from_company_id = company_id(context, row.pop("from_company_id"))
        to_company_id = company_id(context, row.pop("to_company_id"))
        entity = context.make(type_res.schema)
        entity.id = context.make_slug("c2c", row.pop("id"))
        entity.add(type_res.from_prop, from_company_id)
        entity.add(type_res.to_prop, to_company_id)
        entity.add(type_res.desc_prop, type_)
        entity.add("startDate", row.pop("date_established"))
        entity.add("endDate", row.pop("date_finished"))
        context.emit(entity)
        # h.audit_data(row)

    for row in stream_csv("core_company"):
        entity = context.make("Organization")
        entity.id = company_id(context, row.pop("id"))
        if row.pop("state_company") == "TRUE":
            entity.add("topics", "gov.soe")
        entity.add("name", row.pop("name"))
        entity.add("name", row.pop("name_en"))
        entity.add("name", row.pop("name_ru"))
        entity.add("alias", row.pop("short_name"))
        entity.add("alias", row.pop("short_name_en"))
        entity.add("alias", row.pop("short_name_ru"))
        entity.add("alias", row.pop("also_known_as"))
        entity.add("website", row.pop("website"))
        entity.add("incorporationDate", row.pop("founded"))
        entity.add("dissolutionDate", row.pop("closed_on"))
        entity.add_cast("Company", "ogrnCode", row.pop("ogrn_code"))
        entity.add_cast("Company", "registrationNumber", row.pop("edrpou"))
        entity.add("modifiedAt", row.pop("last_change"))

        zip_code = row.pop("zip_code")
        addr = h.make_address(
            context,
            remarks=row.pop("raw_address"),
            street=row.pop("street"),
            street2=row.pop("appt"),
            city=row.pop("city"),
            postal_code=zip_code,
        )
        h.apply_address(context, entity, addr)

        addr = h.make_address(
            context,
            street=row.pop("street_ru"),
            street2=row.pop("appt_ru"),
            city=row.pop("city_ru"),
            postal_code=zip_code,
        )
        h.apply_address(context, entity, addr)

        addr = h.make_address(
            context,
            street=row.pop("street_en"),
            street2=row.pop("appt_en"),
            city=row.pop("city_en"),
            postal_code=zip_code,
        )
        h.apply_address(context, entity, addr)

        context.emit(entity)
        h.audit_data(row)


def crawl(context: Context):
    countries = get_countries(context)
    crawl_people(context, countries)
    crawl_companies(context, countries)
