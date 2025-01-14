from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import PositionCategorisation, categorise


def split_name(name):
    for i in range(len(name)):
        last_name = name[i:].strip()
        if last_name == last_name.upper():
            last_name = last_name.strip()
            first_name = name[:i].strip()
            return first_name, last_name
    return None, None


def crawl_node(
    context: Context, node, position: Entity, categorisation: PositionCategorisation
):
    mep_id = node.findtext(".//id")
    person = context.make("Person")
    person.id = context.make_slug(mep_id)
    url = "http://www.europarl.europa.eu/meps/en/%s" % mep_id
    person.add("sourceUrl", url)
    name = node.findtext(".//fullName")
    person.add("name", name)
    first_name, last_name = split_name(name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    person.add("nationality", node.findtext(".//country"))
    person.add("topics", "role.pep")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )

    context.emit(occupancy)
    context.emit(person)

    party_name = node.findtext(".//nationalPoliticalGroup")
    if party_name not in ["Independent"]:
        party = context.make("Organization")
        party.id = context.make_slug("npg", party_name)
        if party.id is not None:
            party.add("name", party_name)
            party.add("country", node.findtext(".//country"))
            context.emit(party)
            membership = context.make("Membership")
            membership.id = context.make_id(person.id, party.id)
            membership.add("member", person)
            membership.add("organization", party)
            context.emit(membership)

    group_name = node.findtext(".//politicalGroup")
    group = context.make("Organization")
    group.id = context.make_slug("pg", group_name)
    if group.id is not None:
        group.add("name", group_name)
        group.add("country", "eu")
        context.emit(group)
        membership = context.make("Membership")
        membership.id = context.make_id(person.id, group.id)
        membership.add("member", person)
        membership.add("organization", group)
        context.emit(membership)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    position = h.make_position(
        context,
        "Member of the European Parliament",
        country="eu",
        topics=["gov.igo", "gov.legislative"],
    )
    categorisation = categorise(context, position, True)
    context.emit(position)
    for node in doc.findall(".//mep"):
        crawl_node(context, node, position, categorisation)
