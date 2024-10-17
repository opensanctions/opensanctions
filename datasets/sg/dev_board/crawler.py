from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html


def unblock_validator(doc) -> bool:
    return doc.find(".//section[@class='container']") is not None


def crawl(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=3) # remove chache_days
    main_container = doc.find(".//section[@class='profile-listing analytics ']")
    profiles = main_container.findall(".//div[@class='profile-content-container']")
    for profile in profiles:
        name = profile.find(".//h5").text_content().strip()
        role = profile.find(".//p").text_content().strip()
        print(name, role)

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name)
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
            topics=["gov.executive", "gov.national"],
        )

        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(context, person, position)
            if occupancy:
                context.emit(person, target=True)
                context.emit(position)
                context.emit(occupancy)
