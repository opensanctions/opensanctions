import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html


TITLE_REGEX = re.compile(
    r"^(Mr|MR|Ms|Miss|Mrs|Prof|Dr|Professor Sir|Professor|Er. Dr.|Er.|Ar.|Dr.|A/Prof|Clinical A/Prof|Adj A/Prof|Assoc Prof \(Ms\)|Er. Prof.| Er Prof)\.? (?P<name>.+)$"
)
DATA_URLS = [
    "https://www.sgdi.gov.sg/statutory-boards",
    "https://www.sgdi.gov.sg/ministries",
]


def unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='directory-list']") is not None


def crawl_person(context, official, link, ministry, agency, section_name, data_url):
    position = (
        official.find(".//div[@class='rank']")
        .text_content()
        .strip()
    )
    if any(
        keyword in position.lower()
        for keyword in [
            "pa to",
            "assistant",
            "secretary to",
            "please contact",
        ]
    ):
        return
    full_name = (
        official.find(".//div[@class='name']")
        .text_content()
        .strip()
    )
    email = (
        official.find(".//div[@class='email info-contact']")
        .text_content()
        .strip()
    )
    # phone numbers are also available
    is_ministry = (
        data_url == "https://www.sgdi.gov.sg/ministries"
    )
    is_board_member = (
        data_url == "https://www.sgdi.gov.sg/statutory-boards"
        and section_name.lower()
        in {"board members", "council members"}
    )
    if is_ministry:
        position = f"{position} in the {ministry}"
    elif is_board_member:
        position = f"{position} of the Board of the {agency}"
    else:
        position = f"{position} of the {agency}"

    position = re.sub(
        "Board Member of the Board of",
        "Member of the Board of",
        position,
        re.I,
    )
    print(f"Formatted Position: {position}")
    match = TITLE_REGEX.match(full_name)
    title = None
    if match:
        full_name = match.group("name")
        title = match.group(1)

    person = context.make("Person")
    person.id = context.make_id(full_name, position)
    person.add("name", full_name)
    person.add("sourceUrl", link)
    person.add("topics", "role.pep")
    if title is not None:
        person.add("title", title)
    if email is not None:
        if email.startswith("https://"):
            person.add("website", email)
        else:
            person.add("email", email)

    position = h.make_position(
        context,
        name=position,
        country="sg",
        # topics=["gov.executive", "gov.national"],
    )

    categorisation = categorise(context, position, is_pep=True)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)


def crawl_body(context: Context, org_name, link, data_url):
    board_doc = context.fetch_html(
        link,
        cache_days=3,
    )

    org_name_elem = board_doc.find(".//div[@id='agencyName']/h1")
    for br in org_name_elem.xpath(".//br"):
        if br.tail is None:
            br.tail = "\n"
        else:
            br.tail = "\n" + br.tail
    # Get the text content and split it by newline
    org_name = org_name_elem.text_content()
    org_parts = org_name.split("\n")
    ministry = org_parts[0].strip() if len(org_parts) > 0 else ""
    agency = org_parts[1].strip() if len(org_parts) > 1 else ""

    section_headers = board_doc.findall(".//div[@class='section-header']")

    # Iterate through sections and their officials
    for section in section_headers:
        section_name = section.text_content().strip()
        # Identify positions related to the current section
        section_body = section.getnext()
        if section_body is not None:
            officials = section_body.findall(".//li[@id]")
            for official in officials:
                crawl_person(context, official, link,  ministry, agency, section_name, data_url)
    

def crawl(context: Context):
    for url in DATA_URLS:
        data_url = url
        doc = fetch_html(
            context,
            data_url,
            unblock_validator,
            html_source="httpResponseBody",
            cache_days=3,
        )
        bodies = doc.findall(
            ".//div[@class='directory-list']//ul[@class='ministries']//li//a"
        )
        boards_dict = {}
        for board in bodies:
            link = board.get("href")
            org_name = board.text_content().strip()
            if link is None or org_name == "":
                context.log.error(f"No link or name found for {org_name}")
            boards_dict[org_name] = link
        for org_name, link in boards_dict.items():
            if link is not None:
                crawl_body(context, org_name, link, data_url)
