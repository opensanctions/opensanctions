from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus

FIELDS = [
    ("country", ".//h2[@title='Country']"),
    ("name", ".//div[@title='Salutation and Name']"),
    ("position", ".//div[@title='Title']"),
    ("address", ".//div[@title='Address']"),
    ("address", ".//div[@title='City']"),
]


def emit_relation(context, spouse_name, person_id):
    spouse = context.make("Person")
    spouse.id = context.make_id(spouse_name, person_id)
    spouse.add("name", spouse_name)
    spouse.add("topics", "role.rca")
    context.emit(spouse)
    rel = context.make("Family")
    rel.id = context.make_id(person_id, "spouse", spouse.id)
    rel.add("person", person_id)
    rel.add("relative", spouse.id)
    rel.add("relationship", "Spouse")
    context.emit(rel)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    content = doc.findall(".//div[@class='mainContent']")
    assert len(content) == 1
    containers = content[0].findall(".//div[@class='fluid-container']")
    for c in containers:
        name = c.find(".//div[@title='Salutation and Name']").text_content().strip()
        title = c.find(".//div[@title='Title']").text_content().strip()
        spouse = c.find(".//div[@title='Spouse']").text_content().strip()
        mission = c.find(".//div[@title='Mission Title']").text_content().strip()
        # Skip entries without a name
        if not name:
            continue
        person = context.make("Person")
        person.id = context.make_id(name, title)

        for field, xpath in FIELDS:
            elem = c.find(xpath)
            assert elem is not None
            person.add(field, elem.text_content().strip())
        person.add("topics", "role.pep")
        position = h.make_position(
            context,
            name=person.get("position"),
            country=person.get("country"),
            topics=["gov.national", "role.diplo"],
            description=mission,
        )
        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context, person, position, status=OccupancyStatus.CURRENT
            )
            if spouse:
                emit_relation(context, spouse, person.id)
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)
