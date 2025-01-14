from lxml.etree import _Element
from normality import collapse_spaces
from rigour.mime.types import XML
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h

ALIAS_SPLITS = [", ", " (a.k.a.", "; a.k.a. ", "ALIAS: ", "Hebrew: ", "Arabic: "]


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

    entity = context.make("LegalEntity")
    country_code = registry.country.clean(country)
    entity.id = context.make_id(schedule, country_code, entity_name)
    if given_name is not None or last_name is not None or dob is not None:
        entity.add_schema("Person")
        h.apply_name(entity, first_name=given_name, last_name=last_name)
        h.apply_date(entity, "birthDate", dob)
    elif entity_name is not None:
        entity.add("name", entity_name.split("/"))
        # entity.add("incorporationDate", parse_date(dob))
        assert dob is None, (dob, entity_name)

    entity.add("topics", "sanction")
    entity.add("country", country)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", program)
    sanction.add("reason", schedule)
    sanction.add("authorityId", node.findtext("./Item"))
    h.apply_date(sanction, "listingDate", node.findtext("./DateOfListing"))

    names = collapse_spaces(node.findtext("./Aliases"))
    if names is not None:
        for name in h.multi_split(names, ALIAS_SPLITS):
            trim_name = collapse_spaces(name)
            entity.add("alias", trim_name or None)

    context.emit(entity)
    context.emit(sanction)
