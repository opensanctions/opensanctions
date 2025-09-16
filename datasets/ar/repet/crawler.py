import json
from typing import Dict, Optional
from rigour.mime.types import JSON
from normality import normalize

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.un_sc import apply_un_name_list


URL = "https://repet.jus.gob.ar/xml/%s.json"
NAME_QUALITY = {
    "low": "weakAlias",
    "baja": "weakAlias",  # 'baja'=low
    "good": "alias",
    "buena": "alias",  # 'buena'==good
    "alta": "alias",  # 'alta'==high
    "alto": "alias",
    "a k a": "alias",
    "f k a": "previousName",
}
ALIAS_SPLITS = ["original script", ";"]


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
    if date is None:
        return []
    return [date]


def parse_alias(context: Context, entity: Entity, alias: Dict[str, str]):
    quality = alias.pop("QUALITY", None)
    name_prop = NAME_QUALITY[normalize(quality)] if quality else None
    for name in alias.pop("ALIAS_NAME", None).split(";"):
        h.apply_name(
            entity,
            full=name,
            quiet=True,
            name_prop=name_prop,
        )
    context.audit_data(alias, ignore=["NOTE"])


def parse_address(context: Context, data: Dict[str, str]) -> Optional[Entity]:
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
    entity.add("alias", h.multi_split(data.pop("NAME_ORIGINAL_SCRIPT"), ALIAS_SPLITS))

    names = [
        name
        for name in [
            data.pop("FIRST_NAME", None),
            data.pop("SECOND_NAME", None),
            data.pop("THIRD_NAME", None),
            data.pop("FOURTH_NAME", None),
        ]
        if name is not None and name != ""
    ]
    # The names are copied from the UN list, so use the same semantics
    apply_un_name_list(context, entity, names)

    sanction = h.make_sanction(context, entity)
    submitted_on = parse_date(data.pop("SUBMITTED_ON", None))
    listed_on = parse_date(data.pop("LISTED_ON"))
    modified_at = parse_date(data.pop("LAST_DAY_UPDATED"))
    h.apply_dates(entity, "createdAt", submitted_on)
    h.apply_dates(entity, "createdAt", listed_on)
    if modified_at != []:
        h.apply_date(entity, "createdAt", min(modified_at))
        h.apply_date(entity, "modifiedAt", max(modified_at))

    h.apply_dates(sanction, "listingDate", submitted_on)
    h.apply_dates(sanction, "listingDate", listed_on)
    h.apply_dates(sanction, "startDate", listed_on)
    sanction.add("program", data.pop("UN_LIST_TYPE"))
    sanction.add("program", data.pop("LIST_TYPE"))
    sanction.add("unscId", data.pop("REFERENCE_NUMBER"))
    sanction.add("authority", data.pop("SUBMITTED_BY", None))
    context.emit(sanction)
    return entity


def crawl_persons(context: Context):
    for data in fetch(context, "personas"):
        entity = crawl_common(context, data, "personas", "Person")
        entity.add("title", values(data.pop("TITLE", None)))
        entity.add("nationality", values(data.pop("NATIONALITY", None)))
        entity.add("position", values(data.pop("DESIGNATION", None)))
        entity.add("gender", data.pop("GENDER", None))
        entity.add("birthDate", data.pop("DATE_OF_BIRTH", None))
        entity.add("birthDate", data.pop("YEAR", None))
        entity.add("birthPlace", data.pop("CITY_OF_BIRTH", None))
        entity.add("country", data.pop("COUNTRY_OF_BIRTH", None))

        for dob in data.pop("INDIVIDUAL_DATE_OF_BIRTH", []):
            date = parse_date(dob.pop("DATE", None))
            h.apply_dates(entity, "birthDate", date)
            date = parse_date(dob.pop("TYPE_OF_DATE", None))
            h.apply_dates(entity, "birthDate", date)
            entity.add("birthDate", dob.pop("YEAR", None))
            entity.add("birthDate", dob.pop("FROM_YEAR", None))
            entity.add("birthDate", dob.pop("TO_YEAR", None))
            context.audit_data(dob, ignore=["NOTE"])

        for doc in data.pop("INDIVIDUAL_DOCUMENT", []):
            type_ = doc.pop("TYPE_OF_DOCUMENT", None)
            number = doc.pop("NUMBER", None)
            doc_type_res = context.lookup("doc_types", type_)
            if doc_type_res is None:
                context.log.warning(
                    "Unknown document type", type=type_, number=number, doc=doc
                )
                continue

            # Map to a document schema
            elif doc_type_res.document_schema is not None:
                passport = context.make(doc_type_res.document_schema)
                passport.id = context.make_id("ID", entity.id, number)
                passport.add("holder", entity)
                passport.add("type", type_)
                passport.add("number", number)
                passport.add("type", doc.pop("TYPE_OF_DOCUMENT2", None))
                h.apply_dates(
                    passport, "startDate", parse_date(doc.pop("DATE_OF_ISSUE", None))
                )
                passport.add("country", doc.pop("ISSUING_COUNTRY", None))
                passport.add("country", doc.pop("COUNTRY_OF_ISSUE", None))
                passport.add("summary", doc.pop("NOTE", None))
                context.emit(passport)
                context.audit_data(doc, ignore=["CITY_OF_ISSUE"])

            # Map to a prop
            elif doc_type_res.prop is not None:
                entity.add(doc_type_res.prop, number)
                # Make sure that the rest of the data is empty if we just map to a prop
                context.audit_data(doc)
            else:
                context.log.warning(
                    "Invalid doc_type lookup result for type", type=type_
                )

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
            parse_alias(context, entity, alias)

        context.audit_data(data, ["VERSIONNUM"])
        context.emit(entity)


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
            parse_alias(context, entity, alias)

        context.audit_data(data, ["VERSIONNUM"])
        context.emit(entity)


def crawl(context: Context):
    crawl_persons(context)
    crawl_entities(context)
