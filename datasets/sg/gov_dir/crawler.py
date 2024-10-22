import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


TITLE_REGEX = re.compile(
    r"^(Mr|MR|Ms|Miss|Mrs|Prof|Dr|Professor Sir|Professor|Er. Dr.|Er.|Ar.|Dr.|A/Prof|Clinical A/Prof|Adj A/Prof|Assoc Prof \(Ms\)|Er. Prof.| Er Prof|Justice)\.? (?P<name>.+)$"
)
DATA_URLS = [
    "https://www.sgdi.gov.sg/statutory-boards",
    "https://www.sgdi.gov.sg/ministries",
    "https://www.sgdi.gov.sg/organs-of-state",
]


def position_name(body_type, rank, public_body, agency, section_name):
    is_public_body = body_type.lower() == "public body"
    is_board_member = section_name.lower() in {
        "board members",
        "council members",
    }

    if is_public_body and is_board_member:
        position = f"{rank} of the Board of the {public_body}"
    elif is_public_body:
        position = f"{rank} in the {public_body}"
    elif is_board_member:
        position = f"{rank} of the Board of the {agency}"
    else:
        position = f"{rank} of the {agency}"

    position = re.sub(
        r"(Board Member of the Board of|Member of the Board of the Board of)",
        "Member of the Board of",
        position,
        flags=re.I,
    )

    return position


def is_pep(rank: str) -> bool | None:
    rank_lower = rank.lower()

    # Check for PEP indicators
    if any(
        keyword in rank_lower
        for keyword in [
            "minister",
            "president",
            "member",
            "executive officer",
            "director",
            "chairman",
            "ceo",
            "mayor",
            "auditor-general",
            "justice",
            "judge",
        ]
    ):
        return True
    elif any(
        keyword in rank_lower
        for keyword in [
            "pa to",
            "assistant",
            "secretary to",
            "please contact",
        ]
    ):
        return False
    else:
        return None


def crawl_person(context, official, link, public_body, agency, section_name, data_url):
    position = official.find(".//div[@class='rank']").text_content().strip()
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
    full_name = official.find(".//div[@class='name']").text_content().strip()
    email = official.find(".//div[@class='email info-contact']").text_content().strip()
    # phone numbers are also available

    if "ministries" or "organs-of-state" in data_url:
        body_type = "public body"
    elif "statutory-boards" in data_url:
        body_type = "agency"

    pep_status = is_pep(rank=position)  # checking before formatting the position name
    position = position_name(body_type, position, public_body, agency, section_name)

    if section_name.lower() == "members of parliament":  # pep_status and position override for MPs
        pep_status = True
        position = "Member of Parliament"
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
            for email in email.split("; "):
                person.add("email", email)

    position = h.make_position(
        context,
        name=position,
        country="sg",
        # topics=["gov.executive", "gov.national"],
    )
    categorisation = categorise(context, position, is_pep=pep_status)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person, target=True)
            context.emit(position)
            context.emit(occupancy)


def crawl_body(context: Context, org_name, link, data_url):
    board_doc = context.fetch_html(link, cache_days=1)

    org_name_elem = board_doc.find(".//div[@id='agencyName']/h1")
    for br in org_name_elem.xpath(".//br"):
        if br.tail is None:
            br.tail = "\n"
        else:
            br.tail = "\n" + br.tail
    # Get the text content and split it by newline
    org_name = org_name_elem.text_content()
    org_parts = org_name.split("\n")
    public_body = org_parts[0].strip() if len(org_parts) > 0 else ""
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
                crawl_person(
                    context, official, link, public_body, agency, section_name, data_url
                )

    section_info = board_doc.findall(".//div[@class='section-info']")
    for section in section_info:
        for official in section.findall(".//li[@id]"):
            crawl_person(context, official, link, public_body, agency, "", data_url)

    if not any([section_headers, section_info]):
        context.log.error("No officials found", url=link)


def crawl(context: Context):
    for url in DATA_URLS:
        data_url = url
        doc = context.fetch_html(data_url, cache_days=1)
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
