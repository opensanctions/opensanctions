import re
from normality import squash_spaces
from lxml.html import HtmlElement

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

START_DATE_REGEX = re.compile(
    r"(?:Stellvertretendes )?Mitglied des Bundesrates seit (\d{2}\.\d{2}\.\d{4})|,?\s*[Ss]eit (\d{2}\.\d{2}\.\d{4})\s*(?:Stellvertretendes\s+)?Mitglied des Bundesrates"
)
DOB_REGEX = re.compile(
    r"^Geboren(?: am\s+(?:\d{1,2}\.\d{2}\.\d{4}|\d{1,2}\s+\w+\s+\d{4})| (\d{4}))|"
)

# TODO: No details in one of the profiles: https://www.bundesrat.de/SharedDocs/personen/DE/laender/nw/feller-dorothee.html


def extract_date(context, regex, lookup_key, text):
    """Try to extract a date from text using regex, fallback to context lookup, log if missing."""
    match = regex.search(text)
    if match:
        return match.group(1)
    # Fallback to context lookup
    res = context.lookup(lookup_key, text)
    if res:
        return res.value
    context.log.warning(f"No {lookup_key} found in biography.", biography=text)
    return None


def crawl_item(context: Context, item: HtmlElement) -> None:
    url = item.find(".//a").get("href")
    assert url, "No URL found for member"
    member_doc = context.fetch_html(url, cache_days=3)
    details = member_doc.find(".//div[@class='text-box']")
    if "N. N." in details.text_content().strip():
        context.log.info("Skipping member with no name")
        return
    name = details.find(".//h1").text.strip("\xa0|").strip()
    party = details.find(".//span[@class='organization-name']").text.strip()
    position_name = details.find(".//h2").text.strip()
    memberships = [el.text_content().strip() for el in details.findall(".//ul/li")]
    biography_el = member_doc.xpath(
        ".//div[@class='module-box']/h1[text()='Zur Person']/following-sibling::div[@class='row']"
    )
    biography = squash_spaces(biography_el[0].text_content().strip())
    start_date = extract_date(context, START_DATE_REGEX, "start_dates", biography)
    dob = extract_date(context, DOB_REGEX, "birth_dates", biography)

    person = context.make("Person")
    person.id = context.make_id(name, party, position_name)
    person.add("name", name)
    person.add("political", party)
    person.add("position", position_name)
    person.add("description", biography)
    h.apply_date(person, "birthDate", dob)
    for membership in memberships:
        person.add("position", membership.strip())

    position = h.make_position(
        context,
        name="Mitglied des Bundesrates",
        country="de",
        topics=["gov.legislative", "gov.national"],
        lang="deu",
        wikidata_id="Q15835370",
    )
    categorisation = categorise(context, position, True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date if start_date else None,
            categorisation=categorisation,
            no_end_implies_current=False,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=3, absolute_links=True)
    container = doc.find(".//div[@class='row']/ul[@class='members-list']")
    for item in container.xpath(".//li[@class='even' or @class='odd']"):
        crawl_item(context, item)
