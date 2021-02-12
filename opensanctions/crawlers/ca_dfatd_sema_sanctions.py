from pprint import pprint  # noqa
from normality import collapse_spaces

from opensanctions.util import jointext


def crawl(context):
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
    for node in doc.findall(".//record"):
        parse_entry(context, node)


def parse_entry(context, node):
    # ids are per country and entry type (individual/entity)
    country = node.findtext("./Country")
    if " / " in country:
        country, _ = country.split(" / ")
    entity_name = node.findtext("./Entity")
    item = node.findtext(".//Item")

    entity = context.make("LegalEntity")
    if entity_name is None:
        entity = context.make("Person")
    entity.make_id("CASEMA", country, entity_name, item)
    entity.add("name", entity_name)
    entity.add("country", country)

    sanction = context.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "Canadian international sanctions")
    sanction.add("program", node.findtext(".//Schedule"))

    given_name = node.findtext(".//GivenName")
    entity.add("firstName", given_name, quiet=True)
    last_name = node.findtext(".//LastName")
    entity.add("lastName", last_name, quiet=True)
    entity.add("name", jointext(given_name, last_name))

    dob = node.findtext(".//DateOfBirth")
    if dob is not None:
        dob = "-".join(reversed(dob.split("/")))
        entity.add("birthDate", dob, quiet=True)

    names = node.findtext(".//Aliases")
    if names is not None:
        for name in names.split(", "):
            name = collapse_spaces(name)
            entity.add("alias", name)

    context.emit(entity)
    context.emit(sanction)
