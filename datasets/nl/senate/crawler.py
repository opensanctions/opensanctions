import re
from urllib.parse import urljoin
from normality import squash_spaces

from zavod import Context, helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise

DOB_REGEX = re.compile(r"Woonplaats:\s*(.*?)\s*Geboortedatum:\s*([\d-]+)")
TENURE_REGEX = re.compile(r"(.*?)\s*Anciënniteit:\s*(.*)")


def extract_details(context, member, xpath, regex, lookup_keys=None):
    """
    Extracts two fields from a member element. Tries regex first.
    If regex fails, falls back to context.lookup.
    """
    node = member.find(xpath)
    if node is None:
        context.log.warning(f"No node found for xpath: {xpath}")
        return None, None
    full_text = " ".join(child.text.strip() for child in node if child.text)
    match = regex.search(full_text)
    if match:
        return match.group(1), match.group(2)
    if lookup_keys:
        details_res = context.lookup("details", full_text)
        if details_res and details_res.details:
            details = details_res.details[0]
            field1 = details.get(lookup_keys.get(1))
            field2 = details.get(lookup_keys.get(2))
            return field1, field2
    context.log.warning(f"Could not extract fields from: {full_text}")
    return None, None


def crawl_member(context, member):
    a_tag = member.find("./a")
    url = urljoin(context.data_url, a_tag.get("href"))
    name = member.find(".//div[@class='naam']")
    residency, dob = extract_details(
        context,
        member,
        xpath=".//div[@class='cell pasfoto_tekst_k2']",
        regex=DOB_REGEX,
        lookup_keys={1: "place", 2: "dob"},
    )
    # Extract party and tenure
    party, tenure = extract_details(
        context,
        member,
        xpath=".//div[@class='persoon_bijschrift']",
        regex=TENURE_REGEX,
        lookup_keys={1: "party", 2: "tenure"},
    )

    person = context.make("Person")
    person.id = context.make_id("person", name, dob, party)
    person.add("name", squash_spaces(name.text))
    person.add("sourceUrl", url)
    h.apply_date(person, "birthDate", dob)
    person.add("political", party)
    person.add("topics", "role.pep")
    person.add("address", residency)
    context.emit(person)

    position = h.make_position(
        context,
        name="Member of the Senate of the Netherlands",
        country="nl",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
        wikidata_id="Q19305384",
    )
    categorisation = categorise(context, position, True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=False,
            categorisation=categorisation,
            status=OccupancyStatus.UNKNOWN,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=3)
    members = doc.findall(".//li[@class='persoon grid-x nowr']")
    if not members:
        context.log.warning("No members found on the page")
        return
    for member in members:
        crawl_member(context, member)
