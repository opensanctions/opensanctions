from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def get_members_urls(context: Context) -> list:
    main_website = context.fetch_html(context.data_url)

    # this XPath corresponds to the a tags wit the links for a member page
    xpath_to_a_tags = (
        "//*[@id='main']/section/div[2]/div/ul/li/div/div/ul/li[1]/h3/a/@href"
    )
    return h.xpath_strings(main_website, xpath_to_a_tags)


def crawl_item(member_url: str, context: Context):
    member_page_html = context.fetch_html(member_url)

    try:
        name = h.xpath_string(member_page_html, '//*[@id="main"]/header/div/h1/text()')
    except IndexError:
        context.log.info("Couldn't find name. Skipping person.", url=member_url)
        return
    try:
        bio = " ".join(
            h.xpath_strings(
                member_page_html,
                '//*[@class="col-xs-6 profile-desc bg-white content"]/p[text()][1]/text()',
            )
        ).strip()
    except ValueError:
        context.log.warning(
            "Couldn't find biography. Skipping biography.", url=member_url
        )
        return
    try:
        electoral_district = h.xpath_string(
            member_page_html,
            '//*[@class="col-xs-6 profile-desc bg-white content"]//a[contains(@href, "searchByConstituency")]/text()',
        )
    except ValueError:
        context.log.warning(
            "Couldn't find electoral district. Skipping electoral district.",
            url=member_url,
        )
        return

    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("citizenship", "ee")
    entity.add("biography", bio)
    context.log.info(f"Full name {name} unique id {entity.id}")
    h.apply_name(entity, full=name)

    # base the xpath on the email icon to extract the email
    email = h.xpath_string(member_page_html, '//*[@class="icon icon-mail"]/../text()')
    entity.add("email", email)

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
    occupancy.add("constituency", electoral_district)

    context.emit(entity)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    members_urls = get_members_urls(context)

    if members_urls is None:
        return

    for member_url in members_urls:
        crawl_item(member_url, context)
