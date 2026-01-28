import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

DATA_BASE_URL = "https://titania.saeima.lv/personal/deputati/saeima14_depweb_public.nsf"
DEPUTIES_LIST_URL = f"{DATA_BASE_URL}/deputies?OpenView&lang=EN&count=1000"


def crawl_item(context: Context, unid: str) -> None:
    member_url = f"{DATA_BASE_URL}/0/{unid}?OpenDocument&lang=EN"

    member_doc = context.fetch_html(member_url)

    # The title string is starting with two \xa0 characters, which are blank spaces, we will remove them
    full_name = h.xpath_string(member_doc, './/*[@id="ViewBlockTitle"]/text()').replace(
        "\xa0", ""
    )

    entity = context.make("Person")
    entity.id = context.make_slug(full_name)
    h.apply_name(entity, full_name)

    entity.add("sourceUrl", member_url)

    detail_strs = h.xpath_strings(
        member_doc, "//div/span[normalize-space(text()) and not(@class)]/text()[1]"
    )
    if len(detail_strs) == 1:
        h.apply_date(entity, "birthDate", detail_strs[0])
    elif len(detail_strs) == 2:
        # The year is in the second element, the first one is the previous name
        # e.g. '09.12.2019. Name (Previous) Surname'
        h.apply_date(entity, "birthDate", detail_strs[1])
    else:
        context.log.warning(f"Could not find birth date for {full_name}")

    email_el = h.xpath_element(
        member_doc,
        './/*[text()=\'writeJsTrArr("form_email","E-pasta adrese")\']/../../span/a',
    )
    entity.add("email", h.element_text(email_el))

    position = h.make_position(context, "deputy of Saeima", country="lv")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    if occupancy is None:
        return

    context.emit(entity)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    assert context.dataset.url is not None
    overview_page = context.fetch_html(context.dataset.url)
    # Check if the overview page still lists the 14th Saeima in the main menu.
    if (
        len(
            h.xpath_elements(
                overview_page, ".//*[@class='menu']//a[@text()='14th Saeima']"
            )
        )
        == 0
    ):
        context.log.warning(
            "Hardcoded term is no longer current on the overview page, they probably elected a new Saeima.",
        )

    deputies_list_page = context.fetch_html(DEPUTIES_LIST_URL)
    # The links are generated using JS, so we parse the ID from some embedded javascript
    # Using a a regex and build the URL from that.
    members_data = h.xpath_string(
        deputies_list_page, './/*[@class="viewHolderText"]/text()'
    )

    # The lines in the JS code are in this format:
    # drawDep({sname:"Circene",name:"Ingrīda",shortStr:"JV",lst:"THE NEW UNITY parliamentary group",unid:"60440B76C204D1CFC22588E0002AE03F"});
    # we use a regular expression to extract the data
    matches = re.findall(r"unid:\"(?P<unid>[^\"]+)\"", members_data)
    for unid in matches:
        crawl_item(context, unid)
