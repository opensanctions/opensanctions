from lxml import html

from opensanctions import settings
from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import jointext

FORMATS = ["%d.%m.%Y", "%Y%m%d", "%Y-%m-%d"]


async def parse_person(context: Context, node):
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
    await parse_common(context, node, entity)


async def parse_legal(context: Context, node):
    entity = context.make("LegalEntity")
    names = node.findtext("./Name")
    entity.add("name", names.split(", "))
    await parse_common(context, node, entity)


async def parse_common(context: Context, node, entity):
    entity.id = context.make_slug(node.tag, node.findtext("./Number"))
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    inclusion_date = h.parse_date(node.findtext("./DateInclusion"), FORMATS)
    sanction.add("startDate", inclusion_date)
    if inclusion_date is not None:
        entity.context["created_at"] = inclusion_date
    entity.add("topics", "sanction")
    await context.emit(entity, target=True)
    await context.emit(sanction)


async def crawl_index(context: Context):
    params = {"_": settings.RUN_DATE}
    doc = await context.fetch_html(context.dataset.url, params=params)
    for link in doc.findall(".//div[@class='sked-view']//a"):
        href = link.get("href")
        if href.endswith(".xml"):
            return href


async def crawl(context: Context):
    url = await crawl_index(context)
    if url is None:
        context.log.error("Could not locate XML file", url=context.dataset.url)
        return
    path = context.fetch_resource("source.xml", url)
    await context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    xml = context.parse_resource_xml(path)

    for person in xml.findall(".//KyrgyzPhysicPerson"):
        await parse_person(context, person)
    for legal in xml.findall(".//KyrgyzLegalPerson"):
        await parse_legal(context, legal)
