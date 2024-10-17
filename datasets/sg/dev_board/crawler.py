from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html


LINKS = {
    # Mapping URLs to their respective position names
    "https://www.edb.gov.sg/en/about-edb/our-team/executive-management.html": "Member of the Executive Committee",
    "https://www.edb.gov.sg/en/about-edb/our-team/board-members.html": "Board Member",
    "https://www.edb.gov.sg/en/about-edb/our-team/international-advisory-council.html": "Member of the International Advisory Council",
}

def unblock_validator(doc) -> bool:
    return doc.find(".//section[@class='container']") is not None


def emit_person(context: Context, name, role, link, position_name):
    person = context.make("Person")
    person.id = context.make_id(name, role)
    person.add("name", name)
    person.add("position", role)
    if link is not None:
        person.add("sourceUrl", link)
    person.add("topics", "role.pep")

    position = h.make_position(
        context,
        name=position_name,
        country="sg",
        topics=["gov.executive", "gov.national"],
    )

    categorisation = categorise(context, position, is_pep=True)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)

def crawl(context: Context):
    for url, position_name in LINKS.items():
        doc = fetch_html(context, url, unblock_validator, cache_days=3)
        doc.make_links_absolute(url)
        main_containers = doc.findall(".//section[@class='profile-listing analytics ']")
        
        # Loop through each section to find and process the profiles
        for section in main_containers:
            profiles = section.findall(".//div[@class='profile-content-container']")
            for profile in profiles:
                # Extract the name, role, and link
                name = profile.find(".//h5").text_content().strip()
                role = profile.find(".//p").text_content().strip()
                link = profile.find(".//a").get("href") if profile.find(".//a") is not None else None
                emit_person(context, name, role, link, position_name)
