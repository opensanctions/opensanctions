from zavod.util import Element, ElementOrTree

from zavod import Context, Entity
from zavod import helpers as h


def parse_person(context: Context, node: Element) -> None:
    entity = context.make("Person")

    name = node.findtext("./Name")
    patronymic = node.findtext("./Patronomic")
    surname = node.findtext("./Surname")
    entity.id = context.make_id(
        node.tag,
        node.findtext("./DateInclusion"),
        name,
        patronymic,
        surname,
    )
    h.apply_name(
        entity,
        given_name=name,
        patronymic=patronymic if patronymic != "-" else None,
        last_name=surname,
    )
    h.apply_date(entity, "birthDate", node.findtext("./DataBirth"))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    parse_common(context, node, entity)


def parse_legal(context: Context, node: Element) -> None:
    entity = context.make("LegalEntity")
    names = node.findtext("./Name") or ""
    entity.id = context.make_id(node.tag, node.findtext("./DateInclusion"), names)
    entity.add("name", names.split(", "))
    parse_common(context, node, entity)


def parse_common(context: Context, node: Element, entity: Entity) -> None:
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    h.apply_date(sanction, "listingDate", node.findtext("./DateInclusion"))
    h.apply_date(entity, "createdAt", node.findtext("./DateInclusion"))
    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl_index(context: Context) -> str | None:
    assert context.dataset.model.url is not None
    doc = context.fetch_html(context.dataset.model.url, cache_days=1)
    for link in h.xpath_strings(doc, ".//a/@href"):
        if link.endswith(".xml"):
            return link
    return None


def crawl(context: Context) -> None:
    url = crawl_index(context)
    if url is None:
        context.log.error("Could not locate XML file", url=context.dataset.model.url)
        return
    path = context.fetch_resource("source.xml", url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    xml: ElementOrTree = context.parse_resource_xml(path)
    xml = h.remove_namespace(xml)

    for person in xml.findall(".//KyrgyzPhysicPerson"):
        parse_person(context, person)
    for legal in xml.findall(".//KyrgyzLegalPerson"):
        parse_legal(context, legal)
