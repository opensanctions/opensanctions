from banal import first
from lxml import html

from opensanctions import settings
from opensanctions.core import Context
from opensanctions import helpers as h

FORMATS = ["%d.%m.%Y", "%Y%m%d", "%Y-%m-%d"]


def parse_person(context: Context, node):
    entity = context.make("Person")
    h.apply_name(
        entity,
        given_name=node.findtext("./Name"),
        patronymic=node.findtext("./Patronomic"),
        last_name=node.findtext("./Surname"),
    )
    entity.id = context.make_id(
        node.tag,
        node.findtext("./Number"),
        node.findtext("./Name"),
        node.findtext("./Patronomic"),
        node.findtext("./Surname"),
    )
    entity.add("birthDate", h.parse_date(node.findtext("./DataBirth"), FORMATS))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    parse_common(context, node, entity)


def parse_legal(context: Context, node):
    entity = context.make("LegalEntity")
    names = node.findtext("./Name")
    entity.id = context.make_id(node.tag, node.findtext("./Number"), names)
    entity.add("name", names.split(", "))
    parse_common(context, node, entity)


def parse_common(context: Context, node, entity):
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    inclusion_date = h.parse_date(node.findtext("./DateInclusion"), FORMATS)
    sanction.add("startDate", inclusion_date)
    sanction.add("listingDate", inclusion_date)
    entity.add("createdAt", inclusion_date)
    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_index(context: Context):
    params = {"_": settings.RUN_DATE}
    doc = context.fetch_html(context.dataset.url, params=params)
    for link in doc.findall(".//div[@class='sked-view']//a"):
        href = link.get("href")
        if href.endswith(".xml"):
            return href


def crawl(context: Context):
    url = crawl_index(context)
    if url is None:
        context.log.error("Could not locate XML file", url=context.dataset.url)
        return
    path = context.fetch_resource("source.xml", url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    xml = context.parse_resource_xml(path)

    for person in xml.findall(".//KyrgyzPhysicPerson"):
        parse_person(context, person)
    for legal in xml.findall(".//KyrgyzLegalPerson"):
        parse_legal(context, legal)
