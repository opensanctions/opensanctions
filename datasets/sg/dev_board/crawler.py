from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=3) # remove chache_days
    main_container = doc.find(".//div[@class='profile-listing parbase']")

    profiles = main_container.findall(".//div[@class='profile-content-container']")
    for profile in profiles:
        name = profile.find(".//h5") # .text_content().strip()
        role = profile.find(".//p")  #.text_content().strip()
        print(name, role)
        continue
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
