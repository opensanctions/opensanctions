import json
from typing import Dict
from normality import collapse_spaces
from pantomime.types import JSON

from opensanctions.core import Context, Entity
from opensanctions import helpers as h


URL = "https://repet.jus.gob.ar/xml/%s.json"
NAME_QUALITY = {
    "Low": "weakAlias",
    "Good": "alias",
    "a.k.a.": "alias",
    "f.k.a.": "previousName",
    "": None,
}


def values(data):
    if data is None:
        return []
    return [d["VALUE"] for d in data]


def parse_date(date):
    if isinstance(date, list):
        dates = []
        for d in date:
            dates.extend(parse_date(d))
        return dates
    if isinstance(date, dict):
        date = date.get("VALUE")
    return h.parse_date(date, ["%d/%m/%Y"])


def parse_alias(entity: Entity, alias: Dict[str, str]):
    name_prop = NAME_QUALITY[alias.pop("QUALITY", None)]
    h.apply_name(
        entity,
        full=alias.pop("ALIAS_NAME", None),
        quiet=True,
        name_prop=name_prop,
    )
    h.audit_data(alias, ignore=["NOTE"])


def parse_address(context: Context, data):
    return h.make_address(
        context,
        remarks=data.pop("NOTE", None),
        street=data.pop("STREET", None),
        city=data.pop("CITY", None),
        region=data.pop("STATE_PROVINCE", None),
        postal_code=data.pop("ZIP_CODE", None),
        country=data.pop("COUNTRY", None),
    )


def fetch(context: Context, part: str):
    path = context.fetch_resource("%s.json" % part, URL % part)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        return json.load(fh)


def crawl_common(context: Context, data: Dict[str, str], part: str, schema: str):
    entity = context.make(schema)
    entity.id = context.make_slug(part, data.pop("DATAID"))
    entity.add("topics", "sanction")
    entity.add("notes", h.clean_note(data.pop("COMMENTS1")))
    entity.add("notes", h.clean_note(data.pop("NOTE", None)))
    entity.add("alias", data.pop("NAME_ORIGINAL_SCRIPT"))

    h.apply_name(
        entity,
        name1=data.pop("FIRST_NAME", None),
        name2=data.pop("SECOND_NAME", None),
        name3=data.pop("THIRD_NAME", None),
        name4=data.pop("FOURTH_NAME", None),
        quiet=True,
    )

    sanction = h.make_sanction(context, entity)
    submitted_on = parse_date(data.pop("SUBMITTED_ON", None))
    listed_on = parse_date(data.pop("LISTED_ON"))
    entity.add("createdAt", submitted_on or listed_on)
    sanction.add("startDate", listed_on)
    modified_at = parse_date(data.pop("LAST_DAY_UPDATED"))
    sanction.add("modifiedAt", modified_at)
    entity.add("modifiedAt", modified_at)
    sanction.add("program", data.pop("UN_LIST_TYPE"))
    sanction.add("program", data.pop("LIST_TYPE"))
    sanction.add("unscId", data.pop("REFERENCE_NUMBER"))
    sanction.add("reason", data.pop("SUBMITTED_BY", None))
    context.emit(sanction)
    return entity


def crawl_persons(context: Context):
    for data in fetch(context, "personas"):
        entity = crawl_common(context, data, "personas", "Person")
        entity.add("title", values(data.pop("TITLE", None)))
        entity.add("nationality", values(data.pop("NATIONALITY", None)))
        entity.add("position", values(data.pop("DESIGNATION", None)))
        entity.add("gender", h.clean_gender(data.pop("GENDER", None)))
        entity.add("birthDate", data.pop("DATE_OF_BIRTH", None))
        entity.add("birthDate", data.pop("YEAR", None))
        entity.add("birthPlace", data.pop("CITY_OF_BIRTH", None))
        entity.add("country", data.pop("COUNTRY_OF_BIRTH", None))

        for dob in data.pop("INDIVIDUAL_DATE_OF_BIRTH", []):
            date = parse_date(dob.pop("DATE", None))
            entity.add("birthDate", date)
            date = parse_date(dob.pop("TYPE_OF_DATE", None))
            entity.add("birthDate", date)
            entity.add("birthDate", dob.pop("YEAR", None))
            entity.add("birthDate", dob.pop("FROM_YEAR", None))
            entity.add("birthDate", dob.pop("TO_YEAR", None))
            h.audit_data(dob, ignore=["NOTE"])

        for doc in data.pop("INDIVIDUAL_DOCUMENT", []):
            type_ = doc.pop("TYPE_OF_DOCUMENT", None)
            number = doc.pop("NUMBER", None)
            schema = context.lookup_value("doc_types", type_)
            if schema is None:
                context.log.warning("Unknown document type", type=type_)
                continue
            passport = context.make(schema)
            passport.id = context.make_id("ID", entity.id, number)
            passport.add("holder", entity)
            passport.add("type", type_)
            passport.add("number", number)
            passport.add("type", doc.pop("TYPE_OF_DOCUMENT2", None))
            passport.add("startDate", parse_date(doc.pop("DATE_OF_ISSUE", None)))
            passport.add("country", doc.pop("ISSUING_COUNTRY", None))
            passport.add("country", doc.pop("COUNTRY_OF_ISSUE", None))
            passport.add("summary", doc.pop("NOTE", None))
            context.emit(passport)
            h.audit_data(doc, ignore=["CITY_OF_ISSUE"])

        for addr in data.pop("INDIVIDUAL_ADDRESS", []):
            address = parse_address(context, addr)
            h.apply_address(context, entity, address)

        for addr in data.pop("INDIVIDUAL_PLACE_OF_BIRTH", []):
            address = parse_address(context, addr)
            if address is not None:
                entity.add("birthPlace", address.get("full"))
                entity.add("country", address.get("country"))

        for alias in data.pop("INDIVIDUAL_ALIAS", []):
            entity.add("birthDate", alias.pop("DATE_OF_BIRTH", None))
            entity.add("birthDate", alias.pop("YEAR", None))
            entity.add("birthPlace", alias.pop("CITY_OF_BIRTH", None))
            entity.add("country", alias.pop("COUNTRY_OF_BIRTH", None))
            parse_alias(entity, alias)

        h.audit_data(data, ["VERSIONNUM"])
        context.emit(entity, target=True)


def crawl_entities(context: Context):
    for data in fetch(context, "entidades"):
        entity = crawl_common(context, data, "entidades", "Organization")
        entity.add("incorporationDate", data.pop("DATE_OF_BIRTH", None))
        entity.add("incorporationDate", data.pop("YEAR", None))
        data.pop("CITY_OF_BIRTH", None)
        entity.add("country", data.pop("COUNTRY_OF_BIRTH", None))

        for addr in data.pop("ENTITY_ADDRESS", []):
            address = parse_address(context, addr)
            h.apply_address(context, entity, address)

        for alias in data.pop("ENTITY_ALIAS", []):
            entity.add("incorporationDate", alias.pop("DATE_OF_BIRTH", None))
            entity.add("incorporationDate", alias.pop("YEAR", None))
            # entity.add("birthPlace", alias.pop("CITY_OF_BIRTH", None))
            entity.add("country", alias.pop("COUNTRY_OF_BIRTH", None))
            parse_alias(entity, alias)

        h.audit_data(data, ["VERSIONNUM"])
        context.emit(entity, target=True)


def crawl(context: Context):
    crawl_persons(context)
    crawl_entities(context)
