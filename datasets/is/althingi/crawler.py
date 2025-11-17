import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api

EMAIL_PATTERN = r"tpostur\('([^']+)'"
SUFFIX_PATTERN = r"(?<=\d)(st|nd|rd|th)"


def crawl_item(context: Context, *, member_url: str, member_name: str) -> None:
    birth_date_xpath = ".//*[text()='Date of Birth:']/.."
    response = zyte_api.fetch_html(
        context,
        member_url,
        birth_date_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    birth_date = h.element_text(
        h.xpath_elements(response, birth_date_xpath, expect_exactly=1)[0]
    )
    party = h.element_text(
        h.xpath_elements(response, ".//*[text()='Party:']/..", expect_exactly=1)[0]
    )
    telephone = h.element_text(response.find(".//*[@class='tel']"))

    # The email is generate by a script on the page, so we need to extract it from there
    email_script = h.element_text(
        response.find(".//*[@class='contactinfo first notexternal']/li/script")
    )
    match = re.search(EMAIL_PATTERN, email_script)
    if match:
        local_part = match.group(1)
        email = local_part + "@althingi.is"
    else:
        email = None

    person = context.make("Person")
    person.id = context.make_id(member_name, birth_date)

    person.add("name", member_name)
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


def crawl(context: Context) -> None:
    members_xpath = ".//*[@id='members']/tbody/tr/td/b/a"
    response = zyte_api.fetch_html(
        context,
        context.data_url,
        members_xpath,
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    for member_a_tag in response.findall(members_xpath):
        # For some reason the name is not always present on the member page
        # E.g. https://www.althingi.is/altext/cv/en/?nfaerslunr=247
        member_name = h.element_text(member_a_tag)
        member_url = member_a_tag.get("href")
        if member_url is None:
            context.log.error("Could not find URL for member", name=member_name)
            continue
        crawl_item(context, member_url=member_url, member_name=member_name)
