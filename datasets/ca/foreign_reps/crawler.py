import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


REGEX_TITLES = re.compile(
    r"^(?:His Excellency|Her Excellency|Mr\.|Ms\.|Mrs\.)\s+", re.IGNORECASE
)


def strip_title(context, name):
    title_match = REGEX_TITLES.match(name)
    if title_match:
        name = name[title_match.end() :].strip()
    else:
        context.log.warning("Could not match title in name.", name=name)
    return name


def emit_spouse(context, spouse_name, person_id):
    spouse = context.make("Person")
    spouse.id = context.make_id(spouse_name, person_id)
    spouse.add("name", strip_title(context, spouse_name))
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
        country = c.find(".//h2[@title='Country']").text_content().strip()
        title = c.find(".//div[@title='Title']").text_content().strip()
        spouse = c.find(".//div[@title='Spouse']").text_content().strip()
        mission = c.find(".//div[@title='Mission Title']").text_content().strip()
        # Skip entries without a name
        if not name:
            continue
        person = context.make("Person")
        person.id = context.make_id(name, country)
        person.add("name", strip_title(context, name))
        person.add("country", country)
        person.add("topics", "role.pep")
        position = h.make_position(
            context,
            name=title,
            country=country,
            topics=["gov.national", "role.diplo"],
            description=mission,
        )
        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(context, person, position)
            if spouse:
                emit_spouse(context, spouse, person.id)
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)
