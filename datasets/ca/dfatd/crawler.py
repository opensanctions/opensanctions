from lxml.etree import _Element
from normality import collapse_spaces
from pantomime.types import XML
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h

FORMATS = ["%Y", "%d-%m-%Y", "%b-%y"]


def parse_date(date):
    if date is None:
        return None
    return h.parse_date(date.strip(), FORMATS)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for node in doc.findall(".//record"):
        parse_entry(context, node)


def parse_entry(context: Context, node: _Element):
    entity_name = node.findtext("./Entity")
    given_name = node.findtext("./GivenName")
    last_name = node.findtext("./LastName")
    dob = node.findtext("./DateOfBirth")
    schedule = node.findtext("./Schedule")
    if schedule == "N/A":
        schedule = ""
    if entity_name is None:
        entity_name = h.make_name(given_name=given_name, last_name=last_name)
    program = node.findtext("./Country")
    country = program
    if program is not None and "/" in program:
        country, _ = program.split("/", 1)

    item = node.findtext("./Item")
    entity = context.make("LegalEntity")
    country_code = registry.country.clean(country)
    entity.id = context.make_slug(
        schedule,
        item,
        country_code,
        entity_name,
        strict=False,
    )
    if given_name is not None or last_name is not None or dob is not None:
        entity.add_schema("Person")
        h.apply_name(entity, first_name=given_name, last_name=last_name)
        entity.add("birthDate", parse_date(dob))
    elif entity_name is not None:
        entity.add("name", entity_name.split("/"))
        # entity.add("incorporationDate", parse_date(dob))
        assert dob is None, (dob, entity_name)

    entity.add("topics", "sanction")
    entity.add("country", country)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", program)
    sanction.add("reason", schedule)
    sanction.add("authorityId", item)

    names = node.findtext("./Aliases")
    if names is not None:
        for name in h.multi_split(names, [", ", " (a.k.a.", "; a.k.a. ", "ALIAS: ", "Hebrew: ", "Arabic: "]):
            trim_name = collapse_spaces(name)
            entity.add("alias", trim_name or None)
            

    context.emit(entity, target=True)
    context.emit(sanction)
