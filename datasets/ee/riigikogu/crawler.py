from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def get_members_urls(context: Context) -> list:
    main_website = context.fetch_html(context.data_url)

    # this XPath corresponds to the a tags wit the links for a member page
    xpath_to_a_tags = "//*[@id='main']/section/div[2]/div/ul/li/div/div/ul/li[1]/h3/a"

    return [a_tag.get("href") for a_tag in main_website.xpath(xpath_to_a_tags)]


def crawl_item(member_url: str, context: Context):
    member_page_html = context.fetch_html(member_url)

    try:
        name = member_page_html.xpath('//*[@id="main"]/header/div/h1/text()')[0]
    except IndexError:
        context.log.warning("Couldn't find name. Skipping person.", url=member_url)
        return

    entity = context.make("Person")
    entity.id = context.make_id(name)
    context.log.info(f"Full name {name} unique id {entity.id}")
    h.apply_name(entity, full=name)

    try:
        # the phone number apparently is the first contact in the list
        # but to make sure we correctly get it, we based our xpath on the icon
        phone_els = member_page_html.xpath('//*[@class="icon icon-tel"]/../text()')
        phone_number = phone_els[0].replace(" ", "")
        entity.add("phone", phone_number)
    except IndexError:
        # Only log a warning if the name is not "Hele Everaus" or "Kaja Kallas"
        if name not in [
            "Hele Everaus",
            "Kaja Kallas",
            "Mart Võrklaev",
            "Toomas Kivimägi",
        ]:
            context.log.warning("Couldn't find phone number for", name=name)

    try:
        # we do the same as the phone number
        email = member_page_html.xpath('//*[@class="icon icon-mail"]/../text()')[0]
        entity.add("email", email)
    except IndexError:
        context.log.warning("Couldn't find e-mail")

    position = h.make_position(
        context,
        "Member of the Riigikogu",
        country="ee",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, True)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    members_urls = get_members_urls(context)

    if members_urls is None:
        return

    for member_url in members_urls:
        crawl_item(member_url, context)
