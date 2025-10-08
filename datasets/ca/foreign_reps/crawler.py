import re
from typing import Tuple

from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

REGEX_TITLES = re.compile(
    r"^(His Excellency|Her Excellency|Mr\.|Ms\.|Mrs\.)\s+", re.IGNORECASE
)


def split_title(context, name) -> Tuple[str, str | None]:
    title_match = REGEX_TITLES.match(name)
    if title_match:
        name = name[title_match.end() :].strip()
        title = title_match.group(0).strip().replace(".", "")
    else:
        context.log.warning("Could not match title in name.", name=name)
        title = None
    return name, title


def emit_spouse(context, spouse_name, person_id, country):
    spouse = context.make("Person")
    spouse.id = context.make_id(spouse_name, person_id)
    name, title = split_title(context, spouse_name)
    spouse.add("name", name)
    spouse.add("title", title)
    spouse.add("topics", "role.rca")
    spouse.add("country", country)
    spouse.add("country", "ca")
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
        role = c.find(".//div[@title='Title']").text_content().strip()
        spouse = c.find(".//div[@title='Spouse']").text_content().strip()
        # Skip entries without a name
        if not name:
            continue
        person = context.make("Person")
        person.id = context.make_id(name, country)
        name_clean, title = split_title(context, name)
        person.add("name", name_clean)
        person.add("title", title)
        person.add("country", country)
        person.add("country", "ca")
        person.add("topics", "role.pep")
        position = h.make_position(
            context,
            name=f"{role} to Canada",
            country=country,
            topics=["gov.national", "role.diplo"],
        )
        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(context, person, position)
            if spouse:
                emit_spouse(context, spouse, person.id, country)
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)
