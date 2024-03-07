from datetime import datetime
import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise



def crawl_item(unid: str, context: Context):

    member_url = f"https://titania.saeima.lv/personal/deputati/saeima14_depweb_public.nsf/0/{unid}?OpenDocument&lang=EN"

    response = context.fetch_html(member_url)

    # The title string is starting with two \xa0 characters, which are blank spaces, we will remove them
    full_name = response.find('.//*[@id="ViewBlockTitle"]').text.replace(u'\xa0', u'')

    entity = context.make("Person")
    entity.id = context.make_slug(full_name)
    h.apply_name(entity, full_name)

    entity.add("sourceUrl", member_url)

    year_of_birth_el = response.xpath(".//*[text()='writeJsTrArr(\"form_birth_date_year\",\". gadā\")']/..")
    entity.add("birthDate", year_of_birth_el[0].text_content())

    email_el = response.xpath(".//*[text()='writeJsTrArr(\"form_email\",\"E-pasta adrese\")']/../../span/a")
    entity.add("email", email_el[0].text_content())

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

    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)
    

def crawl(context: Context):
    # check if it's time for the end of the term
    if datetime.now().isoformat() > "2025":
        context.log.warning("The 14th Saeima term is nearly over. These occupants will soon not be current.")

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
