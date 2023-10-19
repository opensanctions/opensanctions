from typing import Optional
from normality import collapse_spaces
from lxml.etree import _Element as Element

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.un_sc import get_legal_entities, get_persons

NAME_QUALITY = {
    "Low": "weakAlias",
    "Good": "alias",
    "a.k.a.": "alias",
    "f.k.a.": "previousName",
    "": None,
}


def values(node):
    if node is None:
        return []
    return [c.text for c in node.findall("./VALUE")]


def parse_alias(entity: Entity, node: Element):
    names = node.findtext("./ALIAS_NAME")
    quality = node.findtext("./QUALITY")
    name_prop = NAME_QUALITY[quality]
    if names is None or name_prop is None:
        return

    for name in names.split("; "):
        name = collapse_spaces(name)
        if not len(name):
            continue
        entity.add(name_prop, name)


def parse_address(context: Context, node: Element):
    return h.make_address(
        context,
        remarks=node.findtext("./NOTE"),
        street=node.findtext("./STREET"),
        city=node.findtext("./CITY"),
        region=node.findtext("./STATE_PROVINCE"),
        postal_code=node.findtext("./ZIP_CODE"),
        country=node.findtext("./COUNTRY"),
    )


def parse_entity(context: Context, node: Element, entity: Entity):
    sanction = parse_common(context, entity, node)

    for alias in node.findall("./ENTITY_ALIAS"):
        parse_alias(entity, alias)

    for addr in node.findall("./ENTITY_ADDRESS"):
        h.apply_address(context, entity, parse_address(context, addr))

    context.emit(entity, target=True)
    context.emit(sanction)


def parse_individual(context: Context, node: Element, person: Entity):
    sanction = parse_common(context, person, node)
    person.add("title", values(node.find("./TITLE")))
    person.add("position", values(node.find("./DESIGNATION")))

    for alias in node.findall("./INDIVIDUAL_ALIAS"):
        parse_alias(person, alias)

    for addr in node.findall("./INDIVIDUAL_ADDRESS"):
        h.apply_address(context, person, parse_address(context, addr))

    for doc in node.findall("./INDIVIDUAL_DOCUMENT"):
        country = doc.findtext("./COUNTRY_OF_ISSUE")
        country = country or doc.findtext("./ISSUING_COUNTRY")
        doc_type = doc.findtext("./TYPE_OF_DOCUMENT")
        if doc_type is None:
            continue
        result = context.lookup("document_type", doc_type)
        if result is None:
            context.log.warning(
                "Unknown individual document type",
                doc_type=doc_type,
                number=doc.findtext("./NUMBER"),
                country=country,
            )
            continue
        passport = h.make_identification(
            context,
            person,
            number=doc.findtext("./NUMBER"),
            doc_type=doc_type,
            summary=doc.findtext("./NOTE"),
            start_date=doc.findtext("./DATE_OF_ISSUE"),
            country=country,
            passport=result.passport,
        )
        if passport is not None:
            passport.add("type", doc.findtext("./TYPE_OF_DOCUMENT2"))
            context.emit(passport)

    for nat in node.findall("./NATIONALITY/VALUE"):
        person.add("nationality", nat.text)

    for dob in node.findall("./INDIVIDUAL_DATE_OF_BIRTH"):
        date = dob.findtext("./DATE") or dob.findtext("./YEAR")
        person.add("birthDate", date)

    for pob in node.findall("./INDIVIDUAL_PLACE_OF_BIRTH"):
        address = parse_address(context, pob)
        if address is not None:
            person.add("birthPlace", address.get("full"))
            person.add("country", address.get("country"))

    context.emit(person, target=True)
    context.emit(sanction)


def parse_common(context: Context, entity: Entity, node: Element):
    h.apply_name(
        entity,
        given_name=node.findtext("./FIRST_NAME"),
        second_name=node.findtext("./SECOND_NAME"),
        name3=node.findtext("./THIRD_NAME"),
        name4=node.findtext("./FOURTH_NAME"),
        quiet=True,
    )
    entity.add("alias", node.findtext("./NAME_ORIGINAL_SCRIPT"))
    entity.add("notes", h.clean_note(node.findtext("./COMMENTS1")))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    entity.add("createdAt", node.findtext("./LISTED_ON"))
    sanction.add("listingDate", node.findtext("./LISTED_ON"))
    sanction.add("startDate", node.findtext("./LISTED_ON"))
    sanction.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))
    entity.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))
    sanction.add("program", node.findtext("./UN_LIST_TYPE"))
    sanction.add("unscId", node.findtext("./REFERENCE_NUMBER"))
    return sanction


def crawl_index(context: Context) -> Optional[str]:
    doc = context.fetch_html(context.data_url, cache_days=1)
    for link in doc.findall(".//a"):
        href = link.get("href")
        if href is None or "consolidated_en" not in href:
            continue
        if href.endswith(".xml"):
            return href
    return None


def crawl(context: Context):
    # xml_url = crawl_index(context)
    # if xml_url is None:
    #     raise ValueError("No XML file found on %s" % context.data_url)
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    for node, entity in get_persons(context, context.dataset.prefix, doc):
        parse_individual(context, node, entity)

    for node, entity in get_legal_entities(context, context.dataset.prefix, doc):
        parse_entity(context, node, entity)
