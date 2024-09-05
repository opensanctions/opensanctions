from pathlib import Path
from typing import Generator, List, Optional, Tuple
from enum import Enum
from normality import collapse_spaces
from lxml.etree import _Element as Element
import re

from zavod import Context, Entity
from zavod.meta import load_dataset_from_path
from zavod.meta.dataset import Dataset
from zavod.util import ElementOrTree
from zavod import helpers as h


class Regime(Enum):
    SOMALIA        = re.compile(r"SO.\.\d+")  # SO                 751 (1992)
    DAESH_AL_QAIDA = re.compile(r"QD.\.\d+")  # non-State entity   1267/1989
    IRAQ           = re.compile(r"IQ.\.\d+")  # IQ                 1518 (2003)
    DRC            = re.compile(r"CD.\.\d+")  # CD                 1533 (2004)
    SUDAN          = re.compile(r"SD.\.\d+")  # SD                 1591 (2005)
    NORTH_KOREA    = re.compile(r"KP.\.\d+")  # KP                 1718 (2006)
    LIBYA          = re.compile(r"LY.\.\d+")  # LY                 1970 (2011)
    TALIBAN        = re.compile(r"TA.\.\d+")  # non-State entity   1988 (2011)
    GUINEA_BISSAU  = re.compile(r"GB.\.\d+")  # GB                 2048 (2012)
    CAR            = re.compile(r"CF.\.\d+")  # CF                 2127 (2013)
    YEMEN          = re.compile(r"YE.\.\d+")  # YE                 2140 (2014)
    SOUTH_SUDAN    = re.compile(r"SS.\.\d+")  # SS                 2206 (2015)
    HAITI          = re.compile(r"HT.\.\d+")  # HT                 2653 (2022)


def get_persons(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]] = None,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    yield from get_entities(
        context, prefix, doc, include_prefixes, "INDIVIDUAL", "Person"
    )


def get_legal_entities(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]] = None,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    yield from get_entities(
        context, prefix, doc, include_prefixes, "ENTITY", "LegalEntity"
    )


def get_entities(
    context: Context,
    prefix: str,
    doc: ElementOrTree,
    include_prefixes: Optional[List[str]],
    tag: str,
    schema: str,
) -> Generator[Tuple[ElementOrTree, Entity], None, None]:
    for node in doc.findall(f".//{tag}"):
        perm_ref = node.findtext("./REFERENCE_NUMBER")
        if (
            include_prefixes is None
            or perm_ref is None
            or any([perm_ref.startswith(un_prefix) for un_prefix in include_prefixes])
        ):
            yield node, make_entity(context, prefix, schema, node)


def make_entity(
    context: Context, prefix: str, schema: str, node: ElementOrTree
) -> Entity:
    """Make an entity, set its ID, and add the name and sanction topic so that there is
    at least one property, making it useful and ready to emit."""
    entity = context.make(schema)
    entity.id = context.make_slug(node.findtext("./DATAID"), prefix=prefix)
    h.apply_name(
        entity,
        given_name=node.findtext("./FIRST_NAME"),
        second_name=node.findtext("./SECOND_NAME"),
        name3=node.findtext("./THIRD_NAME"),
        name4=node.findtext("./FOURTH_NAME"),
        quiet=True,
    )
    entity.add("topics", "sanction")
    return entity


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


def parse_entity(context: Context, regimes: List[str], node: Element, entity: Entity):
    if (sanction := parse_common(context, regimes, entity, node)) is None:
        return

    for alias in node.findall("./ENTITY_ALIAS"):
        parse_alias(entity, alias)

    for addr in node.findall("./ENTITY_ADDRESS"):
        h.apply_address(context, entity, parse_address(context, addr))

    context.emit(entity, target=True)
    context.emit(sanction)


def parse_individual(un_sc: Dataset, context: Context, regimes: List[Regime], node: Element, person: Entity):
    if (sanction := parse_common(context, regimes, person, node)) is None:
        return

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
        result = un_sc.lookups["document_type"].match(doc_type)
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


def parse_common(context: Context, regimes: List[Regime], entity: Entity, node: Element) -> Optional[Entity]:
    reference = node.findtext("./REFERENCE_NUMBER")
    # If sanction regime filter is specified, use it, otherwise accept all
    if regimes:
        if not any(regime.value.match(reference) for regime in regimes):
            return None
    entity.add("alias", node.findtext("./NAME_ORIGINAL_SCRIPT"))
    entity.add("notes", h.clean_note(node.findtext("./COMMENTS1")))

    sanction = h.make_sanction(context, entity)
    entity.add("createdAt", node.findtext("./LISTED_ON"))
    sanction.add("listingDate", node.findtext("./LISTED_ON"))
    sanction.add("startDate", node.findtext("./LISTED_ON"))
    sanction.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))
    entity.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))
    sanction.add("program", node.findtext("./UN_LIST_TYPE"))
    sanction.add("unscId", reference)
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


def crawl(context: Context, regimes: List[Regime] = []):
    """
    Crawl the UN SC consolidated sanctions list.

    Args:
        context: The context object.
        regimes: A list of sanction regimes to filter on.
          If empty, all sanctions are accepted.
    """
    un_sc_path = Path(__file__).parent.parent.parent.parent / "datasets/_global/un_sc_sanctions/un_sc_sanctions.yml"
    un_sc = load_dataset_from_path(un_sc_path)
    path = context.fetch_resource("source.xml", un_sc.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    for node, entity in get_persons(context, un_sc.prefix, doc):
        parse_individual(un_sc, context, regimes, node, entity)

    for node, entity in get_legal_entities(context, un_sc.prefix, doc):
        parse_entity(context, regimes, node, entity)
