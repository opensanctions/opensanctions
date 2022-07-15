from lxml.etree import _Element
from normality import collapse_spaces
from pantomime.types import XML

from opensanctions import helpers as h
from opensanctions.core import Context


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for node in doc.findall(".//record"):
        parse_entry(context, node)


def parse_entry(context: Context, node: _Element):
    entity_name = node.findtext("./Entity")
    dob = node.findtext("./DateOfBirth")
    schedule = node.findtext("./Schedule")
    if schedule == "N/A":
        schedule = ""
    program = node.findtext("./Country")
    item = node.findtext("./Item")
    if entity_name is not None:
        entity = context.make("LegalEntity")
        entity.add("name", entity_name.split("/"))
    else:
        entity = context.make("Person")
        given_name = node.findtext("./GivenName")
        last_name = node.findtext("./LastName")
        entity_name = h.make_name(given_name=given_name, last_name=last_name)
        entity.add("name", entity_name)
        entity.add("birthDate", dob)

    country = program
    if program is not None and "/" in program:
        country, _ = program.split("/")
    entity.add("country", country)

    entity.id = context.make_slug(
        schedule,
        item,
        entity.first("country"),
        entity_name,
        strict=False,
    )

    sanction = h.make_sanction(context, entity)
    sanction.add("program", program)
    sanction.add("reason", schedule)
    sanction.add("authorityId", item)

    names = node.findtext("./Aliases")
    if names is not None:
        for name in names.split(", "):
            name = collapse_spaces(name)
            entity.add("alias", name)

    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)
