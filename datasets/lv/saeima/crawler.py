import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

# NOTE: URLs follow a stable pattern across parliamentary terms.
# When the next Saeima is elected, simply increment the number:
# saeima14 -> saeima15 -> saeima16, etc.
NEXT_TERM_URL = "https://titania.saeima.lv/personal/deputati/saeima15_depweb_public.nsf/deputies?OpenView&lang=EN&count=1000"


def crawl_item(unid: str, context: Context):
    member_url = f"https://titania.saeima.lv/personal/deputati/saeima14_depweb_public.nsf/0/{unid}?OpenDocument&lang=EN"

    response = context.fetch_html(member_url)

    # The title string is starting with two \xa0 characters, which are blank spaces, we will remove them
    full_name = response.find('.//*[@id="ViewBlockTitle"]').text.replace("\xa0", "")

    entity = context.make("Person")
    entity.id = context.make_slug(full_name)
    h.apply_name(entity, full_name)

    entity.add("sourceUrl", member_url)

    year = response.xpath(
        "//div/span[normalize-space(text()) and not(@class)]/text()[1]"
    )
    if len(year) == 1:
        h.apply_date(entity, "birthDate", year[0])
    elif len(year) == 2:
        # The year is in the second element, the first one is the previous name
        # e.g. '09.12.2019. Name (Previous) Surname'
        h.apply_date(entity, "birthDate", year[1])
    else:
        context.log.warning(f"Could not find birth date for {full_name}")

    email_el = response.xpath(
        './/*[text()=\'writeJsTrArr("form_email","E-pasta adrese")\']/../../span/a'
    )
    entity.add("email", email_el[0].text_content() if email_el else None)

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


def crawl(context: Context):
    # Check if the next tenure data is available yet
    try:
        future_tenure = context.fetch_html(NEXT_TERM_URL)
        future_text_elem = future_tenure.find('.//*[@class="viewHolderText"]')
        if future_text_elem is not None and future_text_elem.text:
            context.log.warning(
                "Next tenure of Latvian Saeima is available - update crawler!",
                url=NEXT_TERM_URL,
            )
    except Exception as e:
        # Future tenure page doesn't exist yet (expected until next election)
        context.log.debug(f"Future tenure not available yet: {e}")

    response = context.fetch_html(context.data_url)
    # We will first find the link to the page of each member
    # The links are generated using javascript, so we are going
    # to find the id of each member and build the URL from there.
    members_data = response.find('.//*[@class="viewHolderText"]').text

    # The data is in the format:
    # drawDep({sname:"Circene",name:"Ingrīda",shortStr:"JV",lst:"THE NEW UNITY parliamentary group",unid:"60440B76C204D1CFC22588E0002AE03F"});
    # we are goind to use a regular expression to extract the data
    matches = re.findall(r"unid:\"(?P<unid>[^\"]+)\"", members_data)
    for unid in matches:
        crawl_item(unid, context)
