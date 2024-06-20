import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

EMAIL_PATTERN = r"tpostur\('([^']+)'"
SUFFIX_PATTERN = r"(?<=\d)(st|nd|rd|th)"


def crawl_item(member_url: str, name: str, context: Context):

    response = context.fetch_html(member_url)

    birth_date = response.xpath(".//*[text()='Date of Birth:']/..")[0].text_content()
    party = response.xpath(".//*[text()='Party:']/..")[0].text_content()
    telephone = response.find(".//*[@class='tel']").text_content()
    social_media_links = [
        a.get("href") for a in response.findall(".//*[@class='socialmedia']/li/a")
    ]

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
    person.id = context.make_id(name)

    person.add("name", name)
    # The birth date has a suffix that we need to remove
    person.add(
        "birthDate",
        h.parse_date(re.sub(SUFFIX_PATTERN, "", birth_date), formats=["%B %d, %Y"]),
    )
    person.add("phone", telephone)
    person.add("political", party)
    person.add("email", email)

    for link in social_media_links:
        person.add("sourceUrl", link)

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
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    response.make_links_absolute(context.data_url)

    for a_tag in response.findall(".//*[@id='members']/tbody/tr/td/b/a"):
        # For some reason the name is not always present on the member page
        # E.g. https://www.althingi.is/altext/cv/en/?nfaerslunr=247
        crawl_item(a_tag.get("href"), a_tag.text_content(), context)
