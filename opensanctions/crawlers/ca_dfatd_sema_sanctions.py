from normality import collapse_spaces
from pantomime.types import XML

from opensanctions import helpers as h
from opensanctions.core import Context
from opensanctions.util import jointext


async def crawl(context: Context):
    path = await context.fetch_resource("source.xml", context.dataset.data.url)
    await context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for node in doc.findall(".//record"):
        await parse_entry(context, node)


async def parse_entry(context, node):
    entity_name = node.findtext("./Entity")
    if entity_name is not None:
        entity = context.make("LegalEntity")
        entity.add("name", entity_name.split("/"))
    else:
        entity = context.make("Person")
        given_name = node.findtext("./GivenName")
        entity.add("firstName", given_name)
        last_name = node.findtext("./LastName")
        entity.add("lastName", last_name)
        entity.add("name", jointext(given_name, last_name))
        entity.add("birthDate", node.findtext("./DateOfBirth"))

    # ids are per country and entry type (individual/entity)
    item = node.findtext("./Item")
    schedule = node.findtext("./Schedule")
    country = node.findtext("./Country")
    if "/" in country:
        country, _ = country.split("/")
    entity.id = context.make_slug(country, schedule, item, strict=False)
    entity.add("country", country)
    sanction = h.make_sanction(context, entity)
    sanction.add("program", schedule)

    names = node.findtext("./Aliases")
    if names is not None:
        for name in names.split(", "):
            name = collapse_spaces(name)
            entity.add("alias", name)

    entity.add("topics", "sanction")
    await context.emit(entity, target=True)
    await context.emit(sanction)
