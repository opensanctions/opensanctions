# import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

# TITLE_REGEX = re.compile(r"^(Mr|Ms|Miss|Prof|Dr)\.? (?P<name>.+)$")


def unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='directory-list']") is not None


def board_unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='name']") is not None


def crawl(context: Context):
    doc = fetch_html(
        context,
        context.data_url,
        unblock_validator,
        html_source="httpResponseBody",
        cache_days=3,
    )
    statutory_boards = doc.findall(
        ".//div[@class='directory-list']//ul[@class='ministries']//li//a"
    )
    boards_dict = {}
    for board in statutory_boards:
        link = board.get("href")
        org_name = board.text_content().strip()
        if link is None or org_name == "":
            context.log.error(f"No link or name found for {org_name}")
        boards_dict[org_name] = link

    for org_name, link in boards_dict.items():
        print(f"Fetching data for {org_name}: {link}")

        if link is not None:
            board_doc = fetch_html(
                context,
                link,
                board_unblock_validator,
                html_source="httpResponseBody",
                cache_days=3,
            )

            org_name_elem = board_doc.find(".//div[@id='agencyName']/h1")
            for br in org_name_elem.xpath(".//br"):
                if br.tail is None:
                    br.tail = '\n'
                else:
                    br.tail = '\n' + br.tail
            # Get the text content and split it by newline
            org_name = org_name_elem.text_content()
            org_parts = org_name.split("\n")
            ministry = org_parts[0].strip() if len(org_parts) > 0 else ""
            agency = org_parts[1].strip() if len(org_parts) > 1 else ""
 
            section_headers = board_doc.findall(".//div[@class='section-header']")

            # Iterate through sections and their officials
            for section in section_headers:
                section_name = section.text_content().strip()
                print(f"Section header: {section_name}")

                # Identify positions related to the current section
                section_body = section.getnext()
                if section_body is not None:
                    officials = section_body.findall(".//li[@id]")
                    for official in officials:
                        position = (
                            official.find(".//div[@class='rank']")
                            .text_content()
                            .strip()
                        )
                        if any(
                            keyword in position.lower()
                            for keyword in ["pa to", "assistant"]
                        ):
                            continue
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
                        #print(f"Position: {position}")
                        if section_name.lower() == "board members" or section_name.lower() == "council members":
                            position = f"{position} of the Board of {agency} of the {ministry}"
                        elif "Senior Statutory Board Officers" in section_name:
                            position = f"{position} of {agency} of the {ministry}"
                        else:
                            position = f"{position} of {agency} of the {ministry}"

                        print(f"Formatted Position: {position}")
                        person = context.make("Person")
                        person.id = context.make_id(full_name, position)
                        person.add("name", full_name)
                        person.add("sourceUrl", link)
                        person.add("topics", "role.pep")
                        person.add("position", position)
                        if email is not None:
                            person.add("email", email)

                        position = h.make_position(
                            context,
                            name=f"{position} at {org_name}",
                            country="sg",
                            # topics=["gov.executive", "gov.national"],
                        )

                        categorisation = categorise(
                            context, position, is_pep=True
                        )  # adjust
                        if categorisation.is_pep:
                            occupancy = h.make_occupancy(context, person, position)
                            if occupancy:
                                context.emit(person, target=True)
                                context.emit(position)
                                context.emit(occupancy)
