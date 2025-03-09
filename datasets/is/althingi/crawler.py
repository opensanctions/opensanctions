import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

EMAIL_PATTERN = r"tpostur\('([^']+)'"
SUFFIX_PATTERN = r"(?<=\d)(st|nd|rd|th)"


def crawl_item(member_url: str, name: str, context: Context):
    birth_date_xpath = ".//*[text()='Date of Birth:']/.."
    response = fetch_html(
        context,
        member_url,
        birth_date_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    birth_date = response.xpath(birth_date_xpath)[0].text_content()
    party = response.xpath(".//*[text()='Party:']/..")[0].text_content()
    telephone = response.find(".//*[@class='tel']").text_content()

    # The email is generate by a script on the page, so we need to extract it from there
    email_script = response.find(
        ".//*[@class='contactinfo first notexternal']/li/script"
    ).text_content()
    match = re.search(EMAIL_PATTERN, email_script)
    if match:
        local_part = match.group(1)
        email = local_part + "@althingi.is"
    else:
        email = None

    person = context.make("Person")
    person.id = context.make_id(name, birth_date)

    person.add("name", name)
    # The birth date has a suffix that we need to remove
    h.apply_date(person, "birthDate", re.sub(SUFFIX_PATTERN, "", birth_date))
    person.add("phone", telephone)
    person.add("political", party)
    person.add("email", email)

    person.add("sourceUrl", member_url)

    position = h.make_position(context, "Member of the Althing")
    position.add("country", "is")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        True,
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    members_xpath = ".//*[@id='members']/tbody/tr/td/b/a"
    response = fetch_html(
        context,
        context.data_url,
        members_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    response.make_links_absolute(context.data_url)

    for a_tag in response.findall(members_xpath):
        # For some reason the name is not always present on the member page
        # E.g. https://www.althingi.is/altext/cv/en/?nfaerslunr=247
        crawl_item(a_tag.get("href"), a_tag.text_content(), context)
