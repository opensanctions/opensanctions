from lxml import html

from opensanctions import settings
from opensanctions import helpers as h
from opensanctions.util import jointext

FORMATS = ["%d.%m.%Y", "%Y%m%d", "%Y-%m-%d"]


def parse_person(context, node):
    entity = context.make("Person")
    last_name = node.findtext("./Surname")
    entity.add("lastName", last_name)
    first_name = node.findtext("./Name")
    entity.add("firstName", first_name)
    patronymic = node.findtext("./Patronomic")
    entity.add("fatherName", patronymic)
    entity.add("name", jointext(first_name, patronymic, last_name))
    entity.add("birthDate", h.parse_date(node.findtext("./DataBirth"), FORMATS))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    parse_common(context, node, entity)


def parse_legal(context, node):
    entity = context.make("LegalEntity")
    names = node.findtext("./Name")
    entity.add("name", names.split(", "))
    parse_common(context, node, entity)


def parse_common(context, node, entity):
    entity.id = context.make_slug(node.tag, node.findtext("./Number"))
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    inclusion_date = h.parse_date(node.findtext("./DateInclusion"), FORMATS)
    sanction.add("startDate", inclusion_date)
    if inclusion_date is not None:
        entity.context["created_at"] = inclusion_date
    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_index(context):
    params = {"_": settings.RUN_DATE}
    res = context.http.get(context.dataset.url, params=params)
    doc = html.fromstring(res.text)
    for link in doc.findall(".//div[@class='sked-view']//a"):
        href = link.get("href")
        if href.endswith(".xml"):
            return href


def crawl(context):
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
