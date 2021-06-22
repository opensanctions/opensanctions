from lxml import html

from opensanctions import settings
from opensanctions.util import jointext
from opensanctions.util import date_formats, DAY

FORMATS = [("%d.%m.%Y", DAY), ("%Y%m%d", DAY), ("%Y-%m-%d", DAY)]


def parse_date(text):
    return date_formats(text, FORMATS)


def parse_person(context, node):
    entity = context.make("Person")
    last_name = node.findtext("./Surname")
    entity.add("lastName", last_name)
    first_name = node.findtext("./Name")
    entity.add("firstName", first_name)
    patronymic = node.findtext("./Patronomic")
    entity.add("fatherName", patronymic)
    entity.add("name", jointext(first_name, patronymic, last_name))
    entity.add("birthDate", parse_date(node.findtext("./DataBirth")))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    parse_common(context, node, entity)


def parse_legal(context, node):
    entity = context.make("LegalEntity")
    names = node.findtext("./Name")
    entity.add("name", names.split(", "))
    parse_common(context, node, entity)


def parse_common(context, node, entity):
    entity.make_slug(node.tag, node.findtext("./Number"))
    sanction = context.make_sanction(entity)
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    inclusion_date = parse_date(node.findtext("./DateInclusion"))
    sanction.add("startDate", inclusion_date)
    if inclusion_date is not None:
        entity.context["created_at"] = inclusion_date
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
    context.fetch_resource("source.xml", url)
    xml = context.parse_resource_xml("source.xml")

    for person in xml.findall(".//KyrgyzPhysicPerson"):
        parse_person(context, person)
    for legal in xml.findall(".//KyrgyzLegalPerson"):
        parse_legal(context, legal)
